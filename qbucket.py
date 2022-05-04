import random
import math

#QTask class, each qtask has consumption (cores, memory, disk), its task id, and its significance (a scalar)
class QTask:
    def __init__(self, consumption, task_id, significance):
        self.consumption = consumption
        self.task_id = task_id
        self.significance = significance

#QBucket class, this has two apis, get_allocation and add_task, it keeps track of the state of all buckets in system
class QBucket:
    def __init__(self, num_cold_start=10, increase_rate=2, default_res=(1, 1000, 1000), max_res=(12, 24000, 24000), policy='old'):
        self.num_cold_start = num_cold_start    #number of tasks needed to be run first to warm up qbucket, fill it with some values
        self.increase_rate = increase_rate      #rate of increase when we underallocate in cold start phase and when the max in each
                                                #bucket is reached but we still underallocate
        self.default_res = default_res          #default resource allocation/think of it as a first guess
        self.max_res = max_res                  #absolute maximum resource a task can be allocated
        self.sorted_cores = []                  #list of QTasks-cores in order sorted by consumption
        self.sorted_mem = []                    #same as above
        self.sorted_disk = []                   #same as above
        self.buckets_cores = []                 #store current buckets of indices of cores
        self.buckets_mem = []                   #same as above
        self.buckets_disk = []                  #same as above
        self.total_tasks = 0                    #number of tasks so far
        if policy == 'old':
            self.policy = self.__old_policy_cost
        else:
            self.policy = self.__new_policy_cost

    #old partitioning policy
    def __old_policy_cost(self, p1, p2, delim_res, max_res, bot_arr, i, low_index, n1, n2, top_arr, high_index, arr_sig, sorted_res, bot_sig):
        cost_lower_hit = p1*(p1*(delim_res-bot_arr[i-low_index]/n1))
        cost_lower_miss = p1*(p2*(max_res-bot_arr[i-low_index]/n1))
        if n2 == 0:
            cost_upper_hit = 0
            cost_upper_miss = 0
        else:
            cost_upper_miss = p2*(p1*(delim_res+max_res-top_arr[i-low_index]/n2))
            cost_upper_hit = p2*(p2*(max_res-top_arr[i-low_index]/n2))
        delim_cost = cost_lower_hit+cost_lower_miss+cost_upper_miss+cost_upper_hit
        return delim_cost

    #new partitioning policy
    def __new_policy_cost(self, p1, p2, delim_res, max_res, bot_arr, i, low_index, n1, n2, top_arr, high_index, arr_sig, sorted_res, bot_sig):

        #expectation of a new task consumption if it is below delim_res
        exp_cons_lq_delim = sum([arr_sig[j-low_index]*sorted_res[j].consumption/bot_sig[high_index-low_index] for j in range(low_index, i+1)])

        #expectation of cost that new task is lower and our prediction is lower
        cost_lower_hit = p1*(p1*(delim_res-exp_cons_lq_delim))

        #expectation of cost that new task is lower and our prediction is higher
        cost_lower_miss = p1*(p2*(max_res-exp_cons_lq_delim))

        #expectation of a new task consumption if it is above delim_res
        exp_cons_g_delim = sum([arr_sig[j-low_index]*sorted_res[j].consumption/bot_sig[high_index-low_index] for j in range(i+1, high_index+1)])

        #if n2 == 0 then no upper bucket so no cost
        if n2 == 0:
            cost_upper_hit = 0
            cost_upper_miss = 0
        else:

            #expectation of cost that new task is higher and our prediction is lower
            cost_upper_miss = p2*(p1*(delim_res+max_res-exp_cons_g_delim))

            #expectation of cost that new task is higher and our prediction is higher
            cost_upper_hit = p2*(p2*(max_res-exp_cons_g_delim))

        delim_cost = cost_lower_hit+cost_lower_miss+cost_upper_miss+cost_upper_hit
        return delim_cost

    #this method deduplicates values in ret_arr, result_low, and result_high
    def __deduplicate(self, result_low, result_high, ret_arr):

        #deduplicate it
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

        #check if the indices are in increasing order
        for i in range(len(ret_arr)-1):
            if ret_arr[i] < ret_arr[i+1]:
                continue
            else:
                print("Problem when partitioning buckets.")
                exit(1)

        return ret_arr

    #this method partitions a given segment of sorted_resource
    def __bucket_partitioning(self, low_index, high_index, sorted_res):

        #if there's only one element, return it
        if low_index == high_index:
            return [low_index]

        ret_arr = []                            #store indices of buckets' delimiters
                                                #(each bucket only has 1 delimiter)
        num_tasks = high_index - low_index + 1  #number of tasks in this segment
        sum_cons = 0                            #sum of consumption, a temporary variable
        sum_sig = 0                             #sum of significance, a tmp var
        arr_sig = [0]*num_tasks                 #array of all significance, sorted from low
                                                #to high as sorted_res (same ordering)
        bot_sig = [0]*num_tasks                 #sum of significance from bottom to i

        #update arr_sig
        for i in range(low_index, high_index+1):
            val_sig = sorted_res[i].significance
            arr_sig[i-low_index] = val_sig

        #update bot_arr and bot_sig
        bot_arr = [0]*num_tasks                 #sum of consumption from bottom to top
        for i in range(low_index,high_index+1):
            sum_cons += sorted_res[i].consumption
            bot_arr[i-low_index] = sum_cons
            sum_sig += arr_sig[i-low_index]
            bot_sig[i-low_index] = sum_sig

        sum_cons = 0
        top_arr = [0]*num_tasks                 #sum of consumption from top to bottom

        #update top_arr
        for i in range(high_index-1, low_index-1, -1):
            sum_cons += sorted_res[i+1].consumption
            top_arr[i-low_index] = sum_cons

        cost = -1                               #keep track of lowest cost
        ret_index = -1                          #keep track of index with lowest cost
        max_res = sorted_res[high_index].consumption    #maximum resource in this segment

        #loop to calculate cost of partitioning at each point
        for i in range(low_index, high_index+1):
            delim_res = sorted_res[i].consumption       #possible delimiter for new bucket
            n1 = i-low_index+1                          #number of tasks up to this delimiter
            n2 = num_tasks-n1                           #number of tasks above this delimiter, but in this segment
            p1 = bot_sig[i-low_index]/bot_sig[high_index-low_index]     #probability that a task falls into the below possible sub-bucket
            p2 = 1-p1                                                   #probability that a task falls into the above possible sub-bucket

            #calculate delim_cost according to policy
            delim_cost = self.policy(p1, p2, delim_res, max_res, bot_arr, i, low_index, n1, n2, top_arr, high_index, arr_sig, sorted_res, bot_sig)

            #track lowest cost and index of value that results in so
            if cost == -1 or cost > delim_cost:
                cost = delim_cost
                ret_index = i
            else:
                continue

        #if the index value having lowest cost is high_index, then return it as we don't do anymore bucket partitioning
        if ret_index == high_index:
            return [high_index]
        else:
            #get the arrays of indices from trying to divide the two sub-buckets
            result_low = self.__bucket_partitioning(low_index, ret_index, sorted_res)
            result_high = self.__bucket_partitioning(ret_index+1, high_index, sorted_res)

            #deduplicate it
            ret_arr = self.__deduplicate(result_low, result_high, ret_arr)
        return ret_arr

    #get allocation for one type of resource based on its last consumption and whether resource is exceeded
    def __get_allocation_resource(self, res_type, last_res, res_exceeded):

        #get the right type of resource
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

        #if task has never been allocated before
        if last_res == -1:

            #this index represents the index in buckets_res, 0 means consider all buckets
            base_index = 0

        #for maxout tasks
        elif last_res > sorted_res[-1].consumption:
            return self.increase_rate*last_res if self.increase_rate*last_res <= max_res else max_res
        else:
            #we always have base_index initialized as last_res is bounded from above
            #if resource is exceeded, then base index is the one that is strictly
            #larger than last resource, otherwise base index is the first one that is equal to
            #last_resource or larger
            for i in range(len(buckets_res)):
                delim_res_ind = buckets_res[i]
                delim_res = sorted_res[delim_res_ind].consumption
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

        #get number of buckets
        num_buckets_resource = len(buckets_res)

        #get number of completed tasks
        num_completed_tasks = len(self.sorted_cores)

        #each bucket has its weight or probability, which is the sum of all its element
        weighted_bucket_resource = [0]*num_buckets_resource

        last_delim_res_ind = -1         #track last delim res index, initialize to -1 to avoid duplication of index (if it's 0 instead)

        #get each bucket's probability
        for i in range(num_buckets_resource):
            bucket_weight = 0
            delim_res_ind = buckets_res[i]
            if last_delim_res_ind == -1:    #just to avoid corner case of no last delim res index
                for j in range(delim_res_ind+1):
                    bucket_weight += sorted_res[j].significance
            else:
                for j in range(last_delim_res_ind+1, delim_res_ind+1):
                    bucket_weight += sorted_res[j].significance
            weighted_bucket_resource[i] = bucket_weight
            last_delim_res_ind = delim_res_ind

        #total sample space counts from base index
        total_sample_space_resource = sum(weighted_bucket_resource[base_index:])

        #pick randomly a bucket in buckets at least base index
        random_num = random.random()
        cumulative_density = 0
        for i in range(base_index, num_buckets_resource):
            if i == num_buckets_resource - 1:
                delim_res_ind = buckets_res[i]
                return sorted_res[delim_res_ind].consumption
            else:
                cumulative_density += weighted_bucket_resource[i]/total_sample_space_resource
                if random_num <= cumulative_density:
                    delim_res_ind = buckets_res[i]
                    return sorted_res[delim_res_ind].consumption

        #if control is here then not good, return None
        return None

    #a debugging method, never call in real cases
    def __get_buckets_weights(self, resource_type):
        if resource_type == 'cores':
            sorted_res = self.sorted_cores
            buckets_res = self.buckets_cores
        elif resource_type == 'mem':
            sorted_res = self.sorted_mem
            buckets_res = self.buckets_mem
        elif resource_type == 'disk':
            sorted_res = self.sorted_disk
            buckets_res = self.buckets_disk

        #array for weights of buckets
        weight_arr = [0]*len(buckets_res)

        last_delim_res_ind = -1
        #get each bucket's probability
        for i in range(len(buckets_res)):
            bucket_weight = 0
            delim_res_ind = buckets_res[i]
            if last_delim_res_ind == -1:    #just to avoid corner case of no last delim res index
                for j in range(delim_res_ind+1):
                    bucket_weight += sorted_res[j].significance
            else:
                for j in range(last_delim_res_ind+1, delim_res_ind+1):
                    bucket_weight += sorted_res[j].significance
            weight_arr[i] = bucket_weight
            last_delim_res_ind = delim_res_ind
        weight_arr = [e/sum(weight_arr) for e in weight_arr]
        weight_arr = [math.floor(e*100)/100 for e in weight_arr]
        return weight_arr

    #task_prev_res is (cores, mem, disk) if run fails before by running out of reosurces, otherwise None.
    #if task_prev_res is None, and task_res_exceeded is also None
    def get_allocation(self, task_prev_res, task_res_exceeded):

        #get total number of tasks
        total_tasks = len(self.sorted_cores)

        #if task is new
        if task_prev_res is None:

            #if task is in cold start phase, return default res
            if total_tasks < self.num_cold_start:
                return self.default_res  #return default res for new tasks
            else:
                task_prev_res = (-1, -1, -1)    #set some parameters here
                task_res_exceeded = (0, 0, 0)
        else:
            pass

        #get prev task resource and max resource
        tcore = task_prev_res[0]
        tmem = task_prev_res[1]
        tdisk = task_prev_res[2]
        max_core = self.max_res[0]  #max machine
        max_mem = self.max_res[1]
        max_disk = self.max_res[2]

        #if number of completed tasks is less than number of cold starts or already maxed out in all three resources, double both as appropriate and return
        if total_tasks < self.num_cold_start or (tcore >= self.sorted_cores[-1].consumption and tmem >= self.sorted_mem[-1].consumption and tdisk >= self.sorted_disk[-1].consumption):
            tcore = self.increase_rate * tcore if self.increase_rate * tcore <= max_core else max_core
            tmem = self.increase_rate * tmem if self.increase_rate * tmem <= max_mem else max_mem
            tdisk = self.increase_rate * tdisk if self.increase_rate * tdisk <= max_disk else max_disk
            return (tcore, tmem, tdisk)

        #otherwise, this task is out of cold start phase and maxed out at most 2 out of 3 resource types, so allocate each resource indepedently
        tcore = self.__get_allocation_resource('core', tcore, task_res_exceeded[0])
        tmem = self.__get_allocation_resource('mem', tmem, task_res_exceeded[1])
        tdisk = self.__get_allocation_resource('disk', tdisk, task_res_exceeded[2])
        return (tcore, tmem, tdisk)

    #implementing a specific binary insertion sort into sorted Qtask resources
    def __binary_insertion_sort(self, array,element):
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

    #this method adds completed tasks into qbucket
    def add_task(self, task):

        #form Qtask for 3 types of resource
        qcore = QTask(task[0], task[3], task[4])
        qmem = QTask(task[1], task[3], task[4])
        qdisk = QTask(task[2], task[3], task[4])

        #add these QTasks in to sorted_res
        self.sorted_cores = self.__binary_insertion_sort(self.sorted_cores, qcore)
        self.sorted_mem = self.__binary_insertion_sort(self.sorted_mem, qmem)
        self.sorted_disk = self.__binary_insertion_sort(self.sorted_disk, qdisk)

        #increment total tasks
        self.total_tasks += 1

        #update the partitioning of resource buckets
        self.buckets_cores = self.__bucket_partitioning(0, len(self.sorted_cores) - 1, self.sorted_cores)
        self.buckets_mem = self.__bucket_partitioning(0, len(self.sorted_mem) - 1, self.sorted_mem)
        self.buckets_disk = self.__bucket_partitioning(0, len(self.sorted_disk) - 1, self.sorted_disk)

if __name__ == '__main__':
    fake_tasks = []
    qb = QBucket()
    print(dir(qb))
    for i in range(10):
        x = [random.random() for i in range(5)]
        qb.add_task(x)
        print(i, x)
        bucket_and_weight_cores = [(qb.buckets_cores[i], qb._QBucket__get_buckets_weights('cores')[i]) for i in range(len(qb.buckets_cores))]
        bucket_and_weight_mem = [(qb.buckets_mem[i], qb._QBucket__get_buckets_weights('mem')[i]) for i in range(len(qb.buckets_mem))]
        bucket_and_weight_disk =[(qb.buckets_disk[i], qb._QBucket__get_buckets_weights('disk')[i]) for i in range(len(qb.buckets_disk))]
        print([e.consumption for e in qb.sorted_cores])
        print(bucket_and_weight_cores)
        print([e.consumption for e in qb.sorted_mem])
        print(bucket_and_weight_mem)
        print([e.consumption for e in qb.sorted_disk])
        print(bucket_and_weight_disk)
        print(qb.get_allocation(None, None))

