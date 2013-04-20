import re
import threading, thread
import time
import Queue

catg = {
        "MapWorker" : lambda collector, dmt = "[\W\s]": MapWorker(collector, domap, dmt = dmt),
        "ReduceWorker" : lambda collector: ReduceWorker(collector, doreduce),
        }


def domap(args):
    words = args
    records = dict()
    for element in words:
        element = element.lower()
        if ( element in records ):
            records[element] += 1
        else:
            records[element] = 1
    return records

def get_instance_of(type):
    return catg[type]

def partition(fname, delmt = "\n"):
    f = open(fname, "r")
    content = f.read()
    ptn_newline = re.compile(delmt)    
    partitions = ptn_newline.split(content)
    return partitions

class WorkingGroup:
    def __init__(self, collector, thread_num = 5, worker_type = "MapWorker", max_task_num = 0):
        self.collector = collector
        self.queue = Queue.Queue(maxsize = max_task_num)
        self.threads = []
        self.worker_role = worker_type
        self.__init_pool(thread_num)
        self.kicked_off = False        

    def __init_pool(self, thread_num):
        for i in range(thread_num):
            worker = get_instance_of(self.worker_role)(self.collector)
            self.threads.append(WorkSlot(self.queue, worker))
                                                               
    def fill_singletask(self, args):
        self.queue.put(args)

    def fill_tasklist(self, tlist):
        for task in tlist:
            self.queue.put(task)

    def kick_off(self):
        if(not self.kicked_off):
            for t in self.threads:
                    t.start()

    def add_worker(self, num):
        for i in range(num):
            worker = get_instance_of(self.worker_role)(self.collector)
            t = WorkSlot(self.queue, worker)
            t.start()
            self.threads.append(t)
                         
    def wait_all(self):  
        for t in self.threads:  
            if t.isAlive():
                t.join()  



class Collector(list):
    def __init__(self):
        self.count = 0
        self.queue = Queue.Queue()
        self.cp = []

    def gather(self, data):
        self.append(data)
        self.count += 1

    def report(self, timeout = 0):
        while (True):
            time.sleep(4)
            self.cp = self[:]
            del self[:]
            self.cp.sort()
            self.queue.put(self.cp)

    def separate(self, dt):
        keys = dt.keys()
        keys.sort()
        l = len(keys)
        for i in range(len(keys)/5 + 1):
            pack = []
            for j in range(5):
                index = i*5 + j
                if(index - l < 0):
                    k = keys[index]
                    v = dt[k]
                    if k and v:
                        pack.append({k:v})
            if len(pack) > 0: self.send(pack)

    def send(self, pack):
        print "received pack: ", pack
                
            
    def group(self):
        tmp = self.queue.get()
        dt = dict()
        for g in tmp:
            for (k, v) in g.items():
                if k in dt:
                    dt[k].append(v)
                else:
                    dt[k] = [v]
        self.separate(dt)


class Worker:
    def __init__(self, collector, exec_f):
        self.target_collector = collector
        self.action = exec_f

    def do_job(self, args):
        prehandled = self.pre_handle(args)
        resultset = self.action(prehandled)
        if self.check(resultset):
            self.send(resultset)
        thread.exit()

    def send(self, handled_data):
        self.target_collector.gather(handled_data)

    def pre_handle(self, args):
        return args

    def check(self, resultset):
        return False

    
        
            


class MapWorker(Worker):
    def __init__(self, collector, exec_f, dmt = "[\W\s]"):
        Worker.__init__(self, collector, exec_f)
        self.dmt = dmt

    def check(self, resultset):
        return len(resultset) > 0

    def __denoise(self, words, ptn = ""):
        for el in words:
            if re.match("[\s]", el) or el == '':
                words.remove(el)
        return words

    def pre_handle(self, args):
        ptn = re.compile(self.dmt)
        raw_split = ptn.split(args[1])
        return self.__denoise(raw_split)


class ReduceWorker(Worker):
    def __init__(self, collecotr, exec_f):
        Worker.__init__(self, collector, exec_f)

    def check(self, resultset):
        if len(resultset) > 0:
            return True      

class WorkSlot(threading.Thread):
    def __init__(self, queue, worker):
        threading.Thread.__init__(self)
        self.queue = queue
        self.worker = worker

    def run(self):
        while True:
            try:
                args = self.queue.get(block = False)
                if args:
                    self.worker.do_job(args)
                    self.queue.task_done()
                    args = 0
            except:
                break
        


if __name__ == "__main__":
    clt = Collector()
    raw = partition("x:\\Desk\\paper.txt")
    group = WorkingGroup(clt)
    key = 1
    tlist = []
    for part in raw:
        tlist.append((key, part))
        key += 1
    t = threading.Thread(target = clt.report)
    t.start()
    group.fill_tasklist(tlist)
    group.kick_off()
    group.wait_all()
    clt.group()
    