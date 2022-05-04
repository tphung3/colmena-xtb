#import relevant libraries
import pickle as pkl
import argparse
import hashlib
import json
import sys
import logging
import os
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from random import sample, choice, shuffle, random
from datetime import datetime
from functools import partial, update_wrapper
from queue import Queue, Empty
from threading import Event, Lock, Thread, Semaphore
from typing import List, Dict, Tuple, Optional
from traceback import TracebackException
import random
import bisect
import time

import numpy as np
from pydantic import BaseModel
from moldesign.score.mpnn import evaluate_mpnn
from moldesign.simulate.functions import compute_atomization_energy
from moldesign.simulate.specs import lookup_reference_energies, get_qcinput_specification
from moldesign.utils import get_platform_info

from colmena.thinker import BaseThinker, agent
from colmena.method_server import ParslMethodServer
from colmena.redis.queue import ClientQueues, make_queue_pairs
from config import theta_nwchem_config, theta_xtb_config

#class for QBucket task, each task has its consumption, a tuple of (cores, memory, disk), its task id, and its significance value
class QTask:
    def __init__(self, consumption, task_id, significance):
        self.consumption = consumption
        self.task_id = task_id
        self.significance = significance


class QBucket:
    #init a qbucket, hyperparameters are number of cold-start tasks, rate of increase
    #whenever tasks' consumption exceeds their allocation, the default resource guess,
    #and the maximum resource allocation
    def __init__(self, num_cold_start=10, increase_rate=2, default_res=(1, 1000, 1000), max_res=(12,24000,24000)):
        self.num_cold_start = num_cold_start #number of cold starts
        self.increase_rate = increase_rate  #rate of increase from max
        self.default_res = default_res      #default resources allocation/guess
        self.max_res = max_res              #max res, like max machine capacity, good if user just say something huge but not ridiculous
        self.sorted_cores = []              #list of QTasks-cores in order sorted by consumption
        self.sorted_mem = []                #same as above
        self.sorted_disk = []               #same as above
        self.buckets_cores = []             #store current buckets of indices of cores
        self.buckets_mem = []                #same as above
        self.buckets_disk = []               #same as above
        self.total_tasks = 0                #number of tasks so far

    #this function takes in a bucket and maybe split it into multiple buckets
    def bucket_partitioning(self, low_index, high_index, sorted_res):
        if low_index == high_index:
            return [low_index]

        ret_arr = []    #the returning array containing indices of tasks as bucket delimiters
        num_tasks = high_index - low_index + 1 #total number of tasks in this bucket
        sum_cons = 0    
        sum_sig = 0
        arr_sig = [0]*num_tasks #holds all significance values of tasks in this bucket
        bot_sig = [0]*num_tasks #holds incremental sum of significance from bottom to top
        #fill arr_sig
        for i in range(low_index, high_index+1):
            val_sig = sorted_res[i].significance
            arr_sig[i-low_index] = val_sig
        bot_arr = [0]*num_tasks 
        for i in range(low_index,high_index+1):
            sum_cons += sorted_res[i].consumption
            bot_arr[i-low_index] = sum_cons
            sum_sig += arr_sig[i-low_index]
            bot_sig[i-low_index] = sum_sig
        sum_cons = 0
        top_arr = [0]*num_tasks
        for i in range(high_index-1, low_index-1, -1):
            sum_cons += sorted_res[i+1].consumption
            top_arr[i-low_index] = sum_cons
        cost = -1
        ret_index = -1
        max_res = sorted_res[high_index].consumption
        for i in range(low_index, high_index+1):
            delim_res = sorted_res[i].consumption
            n1 = i-low_index+1
            n2 = num_tasks-n1
            p1 = bot_sig[i-low_index]/bot_sig[high_index-low_index]
            p2 = 1-p1
            cost_lower_hit = p1*(p1*(delim_res-bot_arr[i-low_index]/n1))
            cost_lower_miss = p1*(p2*(max_res-bot_arr[i-low_index]/n1))
            if n2 == 0:
                cost_upper_hit = 0
                cost_upper_miss = 0
            else:
                cost_upper_miss = p2*(p1*(delim_res+max_res-top_arr[i-low_index]/n2))
                cost_upper_hit = p2*(p2*(max_res-top_arr[i-low_index]/n2))
            delim_cost = cost_lower_hit+cost_lower_miss+cost_upper_miss+cost_upper_hit
            if cost == -1 or cost > delim_cost:
                cost = delim_cost
                ret_index = i
            else:
                continue
        if ret_index == high_index:
            return [high_index]
        else:
            result_low = self.bucket_partitioning(low_index, ret_index, sorted_res)
            result_high = self.bucket_partitioning(ret_index+1, high_index, sorted_res)
            for i in range(len(result_low)):
                if result_low[i] in ret_arr:
                    continue
                else:
                    ret_arr.append(result_low[i])
            for i in range(len(result_high)):
                if result_high[i] in ret_arr:
                    continue
                else:
                    ret_arr.append(result_high[i])
            for i in range(len(ret_arr)-1):
                if ret_arr[i] < ret_arr[i+1]:
                    continue
                else:
                    print("Problem when partitioning buckets.")
                    exit(1)
        return ret_arr

    def get_allocation_resource(self, res_type, last_res, res_exceeded):
        if res_type == 'core':
            buckets_res = self.buckets_cores
            sorted_res = self.sorted_cores
            max_res = self.max_res[0]
        elif res_type == 'mem':
            buckets_res = self.buckets_mem
            sorted_res = self.sorted_mem
            max_res = self.max_res[1]
        else:
            buckets_res = self.buckets_disk
            sorted_res = self.sorted_disk
            max_res = self.max_res[2]
        if last_res == -1:
            #this index represents the index in buckets_res
            base_index = 0
        #for maxout tasks
        elif last_res > sorted_res[-1].consumption or (last_res == sorted_res[-1].consumption and res_exceeded):
            return self.increase_rate*last_res if self.increase_rate*last_res <= max_res else max_res
        else:
            #we always have base_index initialized as last_res is bounded from above
            for i in range(len(buckets_res)):
                delim_res = buckets_res[i]
                if res_exceeded:
                    if last_res >= delim_res:
                        continue
                    else:
                        base_index = i
                        break
                else:
                    if last_res > delim_res:
                        continue
                    else:
                        base_index = i
                        break
        num_buckets_resource = len(buckets_res)

        num_completed_tasks = len(self.sorted_cores)
        weighted_bucket_resource = [0]*num_buckets_resource
        ptr_weighted_bucket_resource = 0
        ptr_bucket_resource = 0
        cnt_resource = 0
        i = 0
        while i < num_completed_tasks:
            resource = sorted_res[i].consumption
            res_sig = sorted_res[i].significance
            if resource <= buckets_res[ptr_bucket_resource]:
                cnt_resource += res_sig
                if i == num_completed_tasks - 1:
                    weighted_bucket_resource[ptr_weighted_bucket_resource] = cnt_resource
                    for j in range(ptr_weighted_bucket_resource+1, num_buckets_resource):
                        weighted_bucket_resource[j] = res_sig
                i += 1
            else:
                if cnt_resource != 0:
                    weighted_bucket_resource[ptr_weighted_bucket_resource] = cnt_resource
                else:
                    prev_val_sig = sorted_res[i-1].significance
                    weighted_bucket_resource[ptr_weighted_bucket_resource] = prev_val_sig
                ptr_weighted_bucket_resource += 1
                ptr_bucket_resource += 1
                cnt_resource = 0
        total_sample_space_resource = sum(weighted_bucket_resource[base_index:])

        random_num = random.random()
        cumulative_density = 0
        for i in range(base_index, num_buckets_resource):
            if i == num_buckets_resource - 1:
                return buckets_res[i]
            else:
                cumulative_density += weighted_bucket_resource[i]/total_sample_space_resource
                if random_num <= cumulative_density:
                    return buckets_res[i]
        return None

    #task_prev_res is (cores, mem, disk) if run fails before by running out of reosurces, otherwise None.
    #if task_prev_res is None, and task_res_exceeded is also None
    def get_allocation(self, task_prev_res, task_res_exceeded):
        total_tasks = len(self.sorted_cores)
        if task_prev_res is None:
            if total_tasks < self.num_cold_start:
                return self.default_res  #return default res for new tasks
            else:
                task_prev_res = (-1, -1, -1)    #set some parameters here
                task_res_exceeded = (0, 0, 0)
        else:
            pass
        tcore = task_prev_res[0]
        tmem = task_prev_res[1]
        tdisk = task_prev_res[2]
        max_core = self.max_res[0]  #max machine
        max_mem = self.max_res[1]
        max_disk = self.max_res[2]
        #if no completed tasks or already maxed out in all three resources, double both as appropriate and return
        if total_tasks == 0 or (tcore >= self.sorted_cores[-1].consumption and tmem >= self.sorted_mem[-1].consumption and tdisk >= self.sorted_disk[-1].consumption):
            tcore = self.increase_rate * tcore if self.increase_rate * tcore <= max_core else max_core
            tmem = self.increase_rate * tmem if self.increase_rate * tmem <= max_mem else max_mem
            tdisk = self.increase_rate * tdisk if self.increase_rate * tdisk <= max_disk else max_disk
            return (tcore, tmem, tdisk)

        #allocate each resource indepedently
        tcore = self.get_allocation_resource('core', tcore, task_res_exceeded[0])
        tmem = self.get_allocation_resource('mem', tmem, task_res_exceeded[1])
        tdisk = self.get_allocation_resource('disk', tdisk, task_res_exceeded[2])
        return (tcore, tmem, tdisk)

    def binary_insertion_sort(self, array, element):
        if len(array) == 0:
            array.append(element)
            return array
        lo = 0
        hi = len(array) - 1
        while lo < hi:
            mid = (lo+hi)//2
            if array[mid].consumption == element.consumption:
                array.insert(mid, element)
                return array
            elif array[mid].consumption > element.consumption:
                hi = mid - 1
            elif array[mid].consumption < element.consumption:
                lo = mid + 1
            else:
                print("Yikes sorting")
                exit(1)

        if array[lo].consumption > element.consumption:
            array.insert(lo, element)
        else:
            array.insert(lo+1, element)
        return array

    #task is (cores, mem, disk, task_id, significance), resources are consumption wise
    #only add when task finishes successfully
    def add_task(self, task):
        qcore = QTask(task[0], task[3], task[4])
        qmem = QTask(task[1], task[3], task[4])
        qdisk = QTask(task[2], task[3], task[4])
        self.sorted_cores = self.binary_insertion_sort(self.sorted_cores, qcore)
        self.sorted_mem = self.binary_insertion_sort(self.sorted_mem, qmem)
        self.sorted_disk = self.binary_insertion_sort(self.sorted_disk, qdisk)

        #sorted_cores.insert(bisect.bisect_left(sorted_cores, qcore, key=))
        self.total_tasks += 1

        #partitioning buckets
        self.buckets_cores = [self.sorted_cores[i].consumption for i in self.bucket_partitioning(0, len(self.sorted_cores) - 1, self.sorted_cores)]
        self.buckets_mem = [self.sorted_mem[i].consumption for i in self.bucket_partitioning(0, len(self.sorted_mem) - 1, self.sorted_mem)]
        self.buckets_disk = [self.sorted_disk[i].consumption for i in self.bucket_partitioning(0, len(self.sorted_disk) - 1, self.sorted_disk)]
        #self.buckets_mem = self.bucket_partitioning(0, len(self.sorted_cores) - 1, self.sorted_mem)
        #self.buckets_disk = self.bucket_partitioning(0, len(self.sorted_cores) - 1, self.sorted_disk)

#if __name__ == '__main__':
#    qb = QBucket()
    #tc = QTask(14, 10, 1)
    #tm = QTask(1222, 10, 1)
    #td = QTask(6, 10, 1)
    #qb.sorted_cores.append(tc)
    #qb.sorted_mem.append(tm)
    #qb.sorted_disk.append(td)
#    qb.add_task((18, 1240, 6, 9, 1))
#    qb.add_task((18, 1273, 5, 1, 2))
#    qb.add_task((18, 1316, 6, 4, 3))
#    qb.add_task((19, 1241, 5, 2, 4))
#    print(qb.get_allocation((24, 1241, 6), (1, 0, 0)))
#    print("OK")
#    exit(1)

class Thinker(BaseThinker):
    """Screen-then-rank-then-run molecular design loop"""

    def __init__(self, queues: ClientQueues,
                 search_space_path: str,
                 mpnn_paths: List[str],
                 output_dir: str,
                 n_parallel_qc: int,
                 n_parallel_ml: int,
                 molecules_per_ml_task: int,
                 queue_length: int,
                 sampling_fraction: Optional[float],
                 excess_ml_tasks: int):
        """
        Args:
            queues (ClientQueues): Queues to use to communicate with server
            search_path_path: Path to molecule search space
            mpnn_paths: Paths to an ensemble of pre-trained MPNNs
            output_dir (str): Path to the run directory
            molecules_per_ml_task (int): Number of molecules to send with each ML task
            n_parallel_qc (int): Maximum number of QC calculations to perform in parallel
            n_parallel_ml (int): Maximum number of ML calculations to perform in parallel
            queue_length (int): Number of molecules to rank
            sampling_fraction (float): Fraction of search space to explore
            excess_ml_tasks (int): Number of excess tasks to keep in queue above the
                number necessary to keep the ML workers
        """
        super().__init__(queues, daemon=True)

        # Generic stuff: logging, communication to Method Server
        self.output_dir = output_dir

        # The ML components
        self.mpnn_paths = mpnn_paths
        self.search_space_path = search_space_path
        self.sampling_fraction = sampling_fraction

        # Attributes associated with the parallelism/problem size
        self.n_parallel_qc = n_parallel_qc
        self.molecules_per_ml_task = molecules_per_ml_task

        # Compute the number of concurrent ML tasks
        ml_queue_length = n_parallel_ml + excess_ml_tasks

        # Synchronization between the threads
        self.queue_length = queue_length
        self._task_queue = Queue(maxsize=queue_length)
        self._inference_queue = Queue(maxsize=n_parallel_ml)
        self._ml_task_pool = Semaphore(ml_queue_length)
        self._qc_task_pool = Semaphore(self.n_parallel_qc)
        self._ml_tasks_submitted = Event()
        self.n_ml_tasks = 0  # Number active

        #Synch for resource allocation algorithm
        self.task_count_lock = Lock()
        self.task_count = 0

        #resource QBucket algorithm
        #add parameters of QBucket here
        self.qbucket_lock = Lock()
        self.qbucket = QBucket()

        #keep track task for resubmit
        self.task_id_to_task_input = {}

        self.num_cold_start = 10
        self.cold_start_lock = Lock()

    def _write_result(self, result: BaseModel, filename: str, keep_inputs: bool = True, keep_outputs: bool = True):
        """Write result to a log file

        Args:
            result: Result to be written
            filename: Name of the log file
            keep_inputs: Whether to write the function inputs
            keep_outputs: Whether to write the function outputs
        """

        # Determine which fields to dump
        exclude = set()
        if not keep_inputs:
            exclude.add('inputs')
        if not keep_outputs:
            exclude.add('value')

        # Write it out
        with open(os.path.join(self.output_dir, filename), 'a') as fp:
            print(result.json(exclude=exclude), file=fp)

    @agent
    def simulation_consumer(self):
        """Submit and process simulation tasks"""


        # As they come back submit new ones
        i = 0
        while i < self.queue_length:
        #for i in range(self.queue_length):
            # Get the task and store its content
            result = self.queues.get_result(topic='simulate')
            self._qc_task_pool.release()
            self.logger.info(f'Retrieved completed QC task {i+1}/{self.queue_length}')
            #resource_exhaust = result.task_def['resource_exhaust']
            #resource_allocation = result.task_def['resource_allocation']
            #resource_consumption = result.task_def['resource_consumption']
            #self.logger.info(f"Resource report is: exhaust? {resource_exhaust} allocation? {resource_allocation} consumption? {resource_consumption}")
            self.logger.info(f"Resource report is: {result.resource_report} with tag {result.tag}")
            qbucket_internals = f"number of actual tasks done {len(self.qbucket.sorted_cores)}, hint of previous number {self.qbucket.total_tasks}, buckets_cores {self.qbucket.buckets_cores}, buckets_mem {self.qbucket.buckets_mem}, buckets_disk {self.qbucket.buckets_disk}"
            self.logger.info("Internal of qbucket is " + qbucket_internals)
            # Store the content from the previous run
            if result.success:
                # Save the data
                res_exceeded, cons, alloc = result.resource_report
                task_id = int(result.tag)
                tcore = cons[0]
                tmem = cons[1]
                tdisk = cons[2]
                acore = alloc[0]
                amem = alloc[1]
                adisk = alloc[2]
                res_exceeded = 1 if tcore > acore or tmem > amem or tdisk > adisk else 0
                if res_exceeded:
                    s, e, ind = self.task_id_to_task_input[task_id]


                    core_ex = 1 if tcore > acore else 0
                    mem_ex = 1 if tmem > amem else 0
                    disk_ex = 1 if tdisk > adisk else 0
                    with self.qbucket_lock:
                        new_res_request = self.qbucket.get_allocation((tcore, tmem, tdisk), (core_ex, mem_ex, disk_ex))
                    resource_allocation = {'cores': new_res_request[0], 'memory': new_res_request[1], 'disk': new_res_request[2], 'tag': 'compute_atom_energy'}
                    self.queues.send_inputs(s, topic='simulate', method='compute_atomization_energy', keep_inputs=True, task_info={'pred': e, 'rank': ind, 'resource_allocation': resource_allocation, 'tag': task_id})
                else:
                    i += 1
                    with self.qbucket_lock:
                        self.qbucket.add_task((tcore, tmem, tdisk, task_id, i))
                        del self.task_id_to_task_input[task_id]
                    self._write_result(result.value[1], 'qcfractal_records.jsonld')
                    if result.value[2] is not None:
                        self._write_result(result.value[2], 'qcfractal_records.jsonld')
                    result.value = result.value[0]  # Do not store the full results in the database
                    self._write_result(result, 'simulation_records.jsonld', keep_outputs=True)
            else:
                res_exceeded, cons, alloc = result.resource_report
                task_id = int(result.tag)
                tcore = cons[0]
                tmem = cons[1]
                tdisk = cons[2]
                acore = alloc[0]
                amem = alloc[1]
                adisk = alloc[2]
                res_exceeded = 1 if tcore > acore or tmem > amem or tdisk > adisk else 0
                if res_exceeded:
                    s, e, ind = self.task_id_to_task_input[task_id]


                    core_ex = 1 if tcore > acore else 0
                    mem_ex = 1 if tmem > amem else 0
                    disk_ex = 1 if tdisk > adisk else 0
                    with self.qbucket_lock:
                        new_res_request = self.qbucket.get_allocation((tcore, tmem, tdisk), (core_ex, mem_ex, disk_ex))
                    resource_allocation = {'cores': new_res_request[0], 'memory': new_res_request[1], 'disk': new_res_request[2], 'tag': 'compute_atom_energy'}
                    self.queues.send_inputs(s, topic='simulate', method='compute_atomization_energy', keep_inputs=True, task_info={'pred': e, 'rank': ind, 'resource_allocation': resource_allocation, 'tag': task_id})
                else:
                    i += 1
                    with self.qbucket_lock:
                        self.qbucket.add_task((tcore, tmem, tdisk, task_id, i))
                        del self.task_id_to_task_input[task_id]
                    self.logger.warning('Calculation failed! See simulation outputs and Parsl log file')
                    self._write_result(result, 'simulation_records.jsonld', keep_outputs=True)


    @agent
    def search_space_reader(self):
        """Reads search space from disk.

        Separate thread to keep a queue of molecules ready to submit"""

        with open(self.search_space_path) as fp:
            self.logger.info(f'Opened search space molecules from: {self.search_space_path}')

            # Compute the number of entries to pull to get desired sampling rate
            if self.sampling_fraction is None or self.sampling_fraction >= 1.0:
                chunk_size = self.molecules_per_ml_task
            else:
                chunk_size = int(self.molecules_per_ml_task / self.sampling_fraction)

            # Loop until out of molecules
            is_done = False
            while not is_done:
                # Create a chunk
                chunk = [line for line, _ in zip(fp, range(chunk_size))]
                is_done = len(chunk) != chunk_size  # Done if we do not reach the desired chunk size
                if is_done:
                    self.logger.info('Pulled the last batch of molecules')

                # Downsample the chunk to the desired size
                if self.sampling_fraction is not None:
                    desired_size = int(len(chunk) * self.sampling_fraction)
                    self.logger.info(f'Downsampling batch from {len(chunk)} to {desired_size}')
                    chunk = sample(chunk, desired_size)

                # Parse out the SMILES strings
                chunk = [line.strip().split(",")[-1] for line in chunk]  # Molecule is the last entry in line

                # Put it in the queue for the task submitter thread
                self._inference_queue.put(chunk)

            # Put a flag in the queue to say we are done
            self._inference_queue.put(None)


    @agent
    def ml_task_submitter(self):
        self.n_ml_tasks = 0

        # Submit all of the ML tasks
        while True:
            with self.cold_start_lock:
                if self.n_ml_tasks == self.num_cold_start and self.qbucket.total_tasks < self.num_cold_start:
                    #time.sleep(5)
                    continue
            # Get a chunk that is ready to submit
            chunk = self._inference_queue.get()
            if chunk is None:
                self.logger.info('No more inference tasks to submit')
                break

            # Acquire permission to submit to the queue
            #  We do not want too many tasks to be submitted at once to control memory usage
            self._ml_task_pool.acquire()
            with self.qbucket_lock:
                res_request = self.qbucket.get_allocation(None, None)

            resource_allocation={'cores': res_request[0], 'memory': res_request[1], 'disk': res_request[2], 'tag': 'eval_mpnn'}
            with self.task_count_lock:
                self.task_count += 1
                tag = self.task_count
                self.task_id_to_task_input[tag] = (chunk, self.n_ml_tasks)
            self.logger.info(f"ML task with id {tag} is sent for 1st time with resource allocation {resource_allocation}, and its inputs saved.")
            #self.logger.info(f"tag is {tag}")
            self.queues.send_inputs(self.mpnn_paths, chunk, topic='screen', method='evaluate_mpnn', keep_inputs=True,
                                    task_info={'chunk': self.n_ml_tasks, 'chunk_size': len(chunk),
                                        'resource_allocation': resource_allocation,
                                        'tag': tag})
            #self.logger.info(f"hey mpnn paths is {self.mpnn_paths}, chunk is {chunk}, chunk len is {len(chunk)} n_ml_tasks is {self.n_ml_tasks}")

            # Mark that we submitted another batch
            self.n_ml_tasks += 1

        # Mark that we are done
        self.logger.info('Submitted all molecules to inference tasks')
        self._ml_tasks_submitted.set()

    @agent
    def ml_task_consumer(self):
        # Initial list of molecules and their values
        best_mols = []
        best_energies = []

        num_skip = 0

        # Loop until all tasks have been received
        n_received = 0
        while not (self._ml_tasks_submitted.is_set() and n_received == self.n_ml_tasks):
            # Receive a task
            result = self.queues.get_result(topic='screen')
            #resource_exhaust = result.task_def['resource_exhaust']
            #resource_allocation = result.task_def['resource_allocation']
            #resource_consumption = result.task_def['resource_consumption']
            # Mark that it was received and another can be submitted
            self._ml_task_pool.release()
            res_exceeded, cons, alloc = result.resource_report
            task_id = int(result.tag)
            tcore = cons[0]
            tmem = cons[1]
            tdisk = cons[2]
            acore = alloc[0]
            amem = alloc[1]
            adisk = alloc[2]
            res_exceeded = 1 if tcore > acore or tmem > amem or tdisk > adisk else 0
            self.logger.info(f"ML task with id {task_id} is received. It uses {(tcore, tmem, tdisk)} while is allocated with {(acore, amem, adisk)}. res_exceeded variable is {res_exceeded}, its resource report is {result.resource_report}")
            qbucket_internals = f"number of actual tasks done {len(self.qbucket.sorted_cores)}, hint of previous number {self.qbucket.total_tasks}, buckets_cores {self.qbucket.buckets_cores}, buckets_mem {self.qbucket.buckets_mem}, buckets_disk {self.qbucket.buckets_disk}"
            #, sorted_cores {self.qbucket.sorted_cores}, sorted_mem {self.qbucket.sorted_mem}, sorted_disk {self.qbucket.sorted_disk}"
            self.logger.info("Internal of qbucket is " + qbucket_internals)
            #self.logger.info(f'result args for task id {task_id} is {result.args}')
            if tcore > acore and acore == 24:
                n_received += 1
                self.logger.info(f"Task id {task_id} uses too many cores ({tcore}/{acore}). Skipping it...")
            elif res_exceeded:
                chunk, n_ml_tasks = self.task_id_to_task_input[task_id]

                core_ex = 1 if tcore > acore else 0
                mem_ex = 1 if tmem > amem else 0
                disk_ex = 1 if tdisk > adisk else 0
                with self.qbucket_lock:
                    new_res_request = self.qbucket.get_allocation((tcore, tmem, tdisk), (core_ex, mem_ex, disk_ex))
                    resource_allocation = {'cores': new_res_request[0], 'memory': new_res_request[1], 'disk': new_res_request[2], 'tag': 'eval_mpnn'}
                    self.logger.info(f'task {task_id} consumes {(tcore, tmem, tdisk)} is resubmitted with allocation {resource_allocation}')
                    self.queues.send_inputs(self.mpnn_paths, chunk, topic='screen', method='evaluate_mpnn', keep_inputs=True, task_info={'chunk': n_ml_tasks, 'chunk_size': len(chunk), 'resource_allocation': resource_allocation, 'tag': task_id})
            elif len(result.args) < 2:
                num_skip += 1
                self.logger.info(f'ML task id {task_id} doesn\'t have molecules in result.args so skip number is {num_skip}')
                continue
            else:
                n_received += 1
                with self.qbucket_lock:
                    self.qbucket.add_task((tcore, tmem, tdisk, task_id, n_received))
                    del self.task_id_to_task_input[task_id]
                self.logger.info(f'Marked result {n_received}/'
                                f'{self.n_ml_tasks if self._ml_tasks_submitted.is_set() else "?"} as received')

                self.logger.info(f"ML task id {task_id} is completed successfully, resource report is: {result.resource_report} with tag {result.tag}, it is added to self.qbucket")
                #self.logger.info(f"Resource report is: exhaust? {resource_exhaust} allocation? {resource_allocation} consumption? {resource_consumption}")
                # Save the inference result
                self._write_result(result, 'inference_records.jsonld', keep_outputs=False, keep_inputs=False)

                # Find the best molecules
                new_mols = result.args[1]
                new_energies = result.value.mean(axis=1)

                total_mols = np.hstack((best_mols, new_mols))
                total_energies = np.hstack((best_energies, new_energies))

                best_inds = np.argsort(total_energies)[:self.queue_length]
                best_mols = total_mols[best_inds]
                best_energies = total_energies[best_inds]
                self.logger.info(f'Finished updating list to {len(best_mols)} best molecules')

        # We are done ranking all of the molecules, time to submit them!
        for i, (s, e) in enumerate(zip(best_mols, best_energies)):
            # Submit a new QC task (but not more than prescribed amount)
            self._qc_task_pool.acquire()
            #resource_allocation={'cores': 24, 'memory': 32000, 'disk': 4000, 'tag': 'compute_atom_energy'}
            with self.task_count_lock:
                self.task_count += 1
                tag = self.task_count
                self.task_id_to_task_input[tag] = (s, e, i)
            with self.qbucket_lock:
                res_request = self.qbucket.get_allocation(None, None)
            resource_allocation={'cores': res_request[0], 'memory': res_request[1], 'disk': res_request[2], 'tag': 'compute_atom_energy'}
            self.queues.send_inputs(s, topic='simulate', method='compute_atomization_energy', keep_inputs=True,
                                    task_info={'pred': e, 'rank': i,
                                        'resource_allocation': resource_allocation, 'tag': tag})
            self.logger.info(f'Submitted {i}/{len(best_mols)}: {s} with a predicted value of {e}')


if __name__ == '__main__':
    # User inputs
    parser = argparse.ArgumentParser()
    parser.add_argument("--redishost", default="127.0.0.1",
                        help="Address at which the redis server can be reached")
    parser.add_argument("--redisport", default="6379",
                        help="Port on which redis is available")
    parser.add_argument('--mpnn-config-directory', help='Directory containing the MPNN-related JSON files',
                        required=True)
    parser.add_argument('--mpnn-model-files', nargs="+", help='Path to the MPNN h5 files', required=True)
    parser.add_argument('--search-space', help='Path to molecules to be screened', required=True)
    parser.add_argument('--qc-spec', help='Name of the QC specification', required=True,
                        choices=['normal_basis', 'xtb', 'small_basis'])
    parser.add_argument('--qc-parallelism', help='Degree of parallelism for QC tasks. For NWChem, number of nodes per task.'
                        ' For XTB, number of tasks per node.', default=1, type=int)
    parser.add_argument("--parallel-guesses", default=1, type=int,
                        help="Number of calculations to maintain in parallel")
    parser.add_argument("--search-size", default=1000, type=int,
                        help="Number of new molecules to evaluate during this search")
    parser.add_argument("--molecules-per-ml-task", default=10000, type=int,
                        help="Number molecules per inference task")
    parser.add_argument("--sampling-fraction", default=None, type=float,
                        help="Fraction of search space to evaluate")
    parser.add_argument("--ml-prefetch", default=0, help="Number of ML tasks to prefech on each node", type=int)
    parser.add_argument("--ml-excess-queue", default=0, type=int,
                        help="Number of tasks to keep in Colmena work queue beyond what would fill the workers.")

    # Parse the arguments
    args = parser.parse_args()
    run_params = args.__dict__

    # Define the compute setting for the system (only relevant for NWChem)
    #nnodes = int(os.environ.get("COBALT_JOBSIZE", "1"))
    nnodes = 100
    compute_config = {'nnodes': args.qc_parallelism, 'cores_per_rank': 2}

    # Determine the number of QC workers and threads per worker
    if args.qc_spec == "xtb":
        qc_workers = nnodes * args.qc_parallelism
        #compute_config["ncores"] = 64 // args.qc_parallelism
        compute_config["ncores"] = 4 // args.qc_parallelism
    else:
        qc_workers = nnodes // args.qc_parallelism
    run_params["nnodes"] = nnodes
    run_params["qc_workers"] = qc_workers

    # Load in the models, initial dataset, agent and search space
    with open(os.path.join(args.mpnn_config_directory, 'atom_types.json')) as fp:
        atom_types = json.load(fp)
    with open(os.path.join(args.mpnn_config_directory, 'bond_types.json')) as fp:
        bond_types = json.load(fp)

    # Get QC specification
    qc_spec, code = get_qcinput_specification(args.qc_spec)
    if args.qc_spec != "xtb":
        qc_spec.keywords["dft__iterations"] = 150
        qc_spec.keywords["geometry__noautoz"] = True
    ref_energies = lookup_reference_energies(args.qc_spec)

    # Create an output directory with the time and run parameters
    start_time = datetime.utcnow()
    params_hash = hashlib.sha256(json.dumps(run_params).encode()).hexdigest()[:6]
    out_dir = os.path.join('runs', f'{args.qc_spec}-{start_time.strftime("%d%b%y-%H%M%S")}-{params_hash}')
    os.makedirs(out_dir, exist_ok=True)

    # Save the run parameters to disk
    run_params['version'] = 'simple'
    with open(os.path.join(out_dir, 'run_params.json'), 'w') as fp:
        json.dump(run_params, fp, indent=2)
    with open(os.path.join(out_dir, 'qc_spec.json'), 'w') as fp:
        print(qc_spec.json(), file=fp)
    with open(os.path.join(out_dir, 'environment.json'), 'w') as fp:
        json.dump(dict(os.environ), fp, indent=2)

    # Save the platform information to disk
    host_info = get_platform_info()
    with open(os.path.join(out_dir, 'host_info.json'), 'w') as fp:
        json.dump(host_info, fp, indent=2)

    # Set up the logging
    handlers = [logging.FileHandler(os.path.join(out_dir, 'runtime.log')),
                logging.StreamHandler(sys.stdout)]

    class ParslFilter(logging.Filter):
        """Filter out Parsl debug logs"""

        def filter(self, record):
            return not (record.levelno == logging.DEBUG and '/parsl/' in record.pathname)

    for h in handlers:
        h.addFilter(ParslFilter())

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO, handlers=handlers)

    # Write the configuration
    if args.qc_spec == "xtb":
        config = theta_xtb_config(os.path.join(out_dir, 'run-info'), xtb_per_node=args.qc_parallelism, ml_tasks_per_node=1)
    else:
        # ML nodes: N for updating models, 1 for MolDQN, 1 for inference runs
        config = theta_nwchem_config(os.path.join(out_dir, 'run-info'), nodes_per_nwchem=args.qc_parallelism,
                                     ml_prefetch=args.ml_prefetch)

    # Save Parsl configuration
    with open(os.path.join(out_dir, 'parsl_config.txt'), 'w') as fp:
        print(str(config), file=fp)

    # Connect to the redis server
    client_queues, server_queues = make_queue_pairs(args.redishost, args.redisport,
                                                    serialization_method="pickle",
                                                    topics=['simulate', 'screen'],
                                                    keep_inputs=False)

    # Apply wrappers to functions to affix static settings
    #  Update wrapper changes the __name__ field, which is used by the Method Server
    #  TODO (wardlt): Have users set the method name explicitly
    my_compute_atomization = partial(compute_atomization_energy, compute_hessian=args.qc_spec != "xtb",
                                     qc_config=qc_spec, reference_energies=ref_energies,
                                     compute_config=compute_config, code=code)
    my_compute_atomization = update_wrapper(my_compute_atomization, compute_atomization_energy)

    #my_evaluate_mpnn = partial(evaluate_mpnn, atom_types=atom_types, bond_types=bond_types, batch_size=512, n_jobs=64)
    #my_evaluate_mpnn = partial(evaluate_mpnn, atom_types=atom_types, bond_types=bond_types, batch_size=512, n_jobs=4)
    my_evaluate_mpnn = partial(evaluate_mpnn, atom_types=atom_types, bond_types=bond_types, batch_size=512, n_jobs=1)
    my_evaluate_mpnn = update_wrapper(my_evaluate_mpnn, evaluate_mpnn)

    # Create the method server and task generator
    ml_cfg = {'executors': ['ml']}
    #dft_cfg = {'executors': ['qc']}
    dft_cfg = {'executors': ['ml']}
    doer = ParslMethodServer([(my_evaluate_mpnn, ml_cfg), (my_compute_atomization, dft_cfg)],
                             server_queues, config)

    # Compute the number of excess tasks
    excess_tasks = nnodes * args.ml_prefetch + args.ml_excess_queue

    # Configure the "thinker" application
    thinker = Thinker(client_queues,
                      args.search_space,
                      args.mpnn_model_files,
                      out_dir,
                      qc_workers,
                      nnodes,
                      args.molecules_per_ml_task,
                      args.search_size,
                      args.sampling_fraction,
                      excess_tasks)
    logging.info('Created the method server and task generator')

    try:
        # Launch the servers
        #  The method server is a Thread, so that it can access the Parsl DFK
        #  The task generator is a Thread, so that all debugging methods get cast to screen
        doer.start()
        thinker.start()
        logging.info(f'Running on {os.getpid()}')
        logging.info('Launched the servers')

        # Wait for the task generator to complete
        thinker.join()
        logging.info('Task generator has completed')
    finally:
        client_queues.send_kill_signal()

    # Wait for the method server to complete
    doer.join()
