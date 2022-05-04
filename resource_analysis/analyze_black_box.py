import resource
import matplotlib.pyplot as plt
import copy
import sys
#a mapping from task id to [res_cons, [res_alloc1, res_alloc2, ...], report:str]
#task_id is init to [None, {}, None]
task_map = {}

av_core_eff = []
av_mem_eff = []
av_disk_eff = []
total_core_cons = 0
total_mem_cons = 0
total_disk_cons = 0
total_core_alloc = 0
total_mem_alloc = 0
total_disk_alloc = 0


av_core_eff_t = []
av_mem_eff_t = []
av_disk_eff_t = []
total_core_cons_t = 0
total_mem_cons_t = 0
total_disk_cons_t = 0
total_core_alloc_t = 0
total_mem_alloc_t = 0
total_disk_alloc_t = 0

with open(sys.argv[1], 'r') as f:

    for line in f:
        #received task ML
        if len(line.split('is received')) > 1:
            task_id = int(line.split('ML task with id ')[1].split(' is received')[0])
            cons = [int(e) for e in line.split('It uses (')[1].split(')')[0].split(', ')]
            alloc = [int(e) for e in line.split('allocated with (')[1].split(')')[0].split(', ')]
            report = line.split('resource report is ')[1].split('\n')[0]
            time = int(report.split(', ')[5])/1000000 #in microseconds to seconds
            cons.append(time)
            alloc.append(time)
            resource_exceeded = 1 if cons[0] > alloc[0] or cons[1] > alloc[1] or cons[2] > alloc[2] else 0
            assert(int(line.split('res_exceeded variable is ')[1].split(',')[0]) == resource_exceeded)
            #task doesn't overconsume
            if int(line.split('res_exceeded variable is ')[1].split(',')[0]) == 0:
                if task_id in task_map:
                    task_map[task_id][0] = cons
                    task_map[task_id][1].append(alloc)
                    task_map[task_id][2] = report
                else:
                    task_map[task_id] = [cons, [alloc], report]
            else:
                if task_id in task_map:
                    task_map[task_id][0] = None
                    task_map[task_id][1].append(alloc)
                    task_map[task_id][2] = None
                else:
                    task_map[task_id] = [None, [alloc], None]
        #receive task QC
        elif len(line.split('Resource report is')) > 1:
            task_id = int(line.split('tag ')[1])
            cons = [int(e) for e in line.split(', [')[1].split(', ')[:3]]
            alloc = [int(e) for e in line.split("'], [")[1].split(']')[0].split(', ')]
            report = line.split('Resource report is: ')[1].split(' with tag ')[0]
            time = int(report.split(', ')[5])/1000000 #in microseconds to seconds
            cons.append(time)
            alloc.append(time)
            #task doesn't overconsume
            resource_exceeded = 1 if cons[0] > alloc[0] or cons[1] > alloc[1] or cons[2] > alloc[2] else 0
            # if task_id == 256:
            #     xx = 1
            if not resource_exceeded:
                if task_id in task_map:
                    task_map[task_id][0] = cons
                    task_map[task_id][1].append(alloc)
                    task_map[task_id][2] = report
                else:
                    task_map[task_id] = [cons, [alloc], report]
            else:
                if task_id in task_map:
                    task_map[task_id][0] = None
                    task_map[task_id][1].append(alloc)
                    task_map[task_id][2] = None
                else:
                    task_map[task_id] = [None, [alloc], None]
print("Total tasks: ", len(task_map))

#print all report
#map_keys = copy.deepcopy(task_map.keys())
del_keys = []
for e in task_map.keys():
    removed = '<-----------' if task_map[e][0] == None else ''
    print(f'{e} -> {task_map[e]} {removed}')
    if removed != '':
        del_keys.append(e)
    #del task_map[e]
for e in del_keys:
    del task_map[e]
print("Total cleaned tasks: ", len(task_map))

#average
for e in task_map.keys():
    cons, list_alloc, report = task_map[e]
    task_core_cons = cons[0]
    task_mem_cons = cons[1]
    task_disk_cons = cons[2]
    task_core_alloc = 0
    task_mem_alloc = 0
    task_disk_alloc = 0
    for a in list_alloc:
        task_core_alloc += a[0]
        task_mem_alloc += a[1]
        task_disk_alloc += a[2]
    av_core_eff.append(task_core_cons/task_core_alloc)
    if task_core_cons/task_core_alloc > 1 or task_mem_cons/task_mem_alloc > 1 or task_disk_cons/task_disk_alloc > 1:
        print(str(e) + repr(task_map[e]))
        exit(1)
    av_mem_eff.append(task_mem_cons/task_mem_alloc)
    av_disk_eff.append(task_disk_cons/task_disk_alloc)

print(f'Average stats: cores: {sum(av_core_eff)/len(av_core_eff)}, mem: {sum(av_mem_eff)/len(av_mem_eff)}, disk: {sum(av_disk_eff)/len(av_disk_eff)}')

#absolute
for e in task_map.keys():
    cons, list_alloc, report = task_map[e]
    total_core_cons += cons[0]
    total_mem_cons += cons[1]
    total_disk_cons += cons[2]

    task_core_alloc = 0
    task_mem_alloc = 0
    task_disk_alloc = 0
    for a in list_alloc:
        task_core_alloc += a[0]
        task_mem_alloc += a[1]
        task_disk_alloc += a[2]
    total_core_alloc += task_core_alloc
    total_mem_alloc += task_mem_alloc
    total_disk_alloc += task_disk_alloc

print(f'Absolute stats: cores: {total_core_cons/total_core_alloc}, mem: {total_mem_cons/total_mem_alloc}, disk: {total_disk_cons/total_disk_alloc}')

#average time
for e in task_map.keys():
    cons, list_alloc, report = task_map[e]
    task_core_cons = cons[0]*cons[3]
    task_mem_cons = cons[1]*cons[3]
    task_disk_cons = cons[2]*cons[3]
    task_core_alloc = 0
    task_mem_alloc = 0
    task_disk_alloc = 0
    for a in list_alloc:
        task_core_alloc += a[0]*cons[3]
        task_mem_alloc += a[1]*cons[3]
        task_disk_alloc += a[2]*cons[3]
    av_core_eff_t.append(task_core_cons/task_core_alloc)
    av_mem_eff_t.append(task_mem_cons/task_mem_alloc)
    av_disk_eff_t.append(task_disk_cons/task_disk_alloc)

print(f'Average stats with time: cores: {sum(av_core_eff_t)/len(av_core_eff_t)}, mem: {sum(av_mem_eff_t)/len(av_mem_eff_t)}, disk: {sum(av_disk_eff_t)/len(av_disk_eff_t)}')

#absolute time
for e in task_map.keys():
    cons, list_alloc, report = task_map[e]
    total_core_cons_t += cons[0]*cons[3]
    total_mem_cons_t += cons[1]*cons[3]
    total_disk_cons_t += cons[2]*cons[3]

    task_core_alloc = 0
    task_mem_alloc = 0
    task_disk_alloc = 0
    for a in list_alloc:
        task_core_alloc += a[0]*cons[3]
        task_mem_alloc += a[1]*cons[3]
        task_disk_alloc += a[2]*cons[3]
    total_core_alloc_t += task_core_alloc
    total_mem_alloc_t += task_mem_alloc
    total_disk_alloc_t += task_disk_alloc

print(f'Absolute stats with time: cores: {total_core_cons_t/total_core_alloc_t}, mem: {total_mem_cons_t/total_mem_alloc_t}, disk: {total_disk_cons_t/total_disk_alloc_t}')

plt.plot(range(len(av_mem_eff)), sorted(av_mem_eff))
plt.savefig('av_mem_eff_black_box.png')
