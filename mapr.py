import re
import threading, thread
import time
import Queue

def get_instance_of(type):
    catg = {"MapWorker": lambda collector, dmt = "[\W\s]": MapWorker(collector, dmt = dmt)}
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
                                                               
    def fill_single(self, args):
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
            time.sleep(8)
            self.cp = self[:]
            del self[:]
            self.cp.sort()
            self.queue.put(self.cp)

    def separate(self, dt):
        keys = dt.keys()
        keys.sort()

            
    def group(self):
        print "sepa!"
        tmp = self.queue.get()
        print tmp
        dt = dict()
        for g in tmp:
            for (k, v) in g.items():
                if k in dt:
                    dt[k].append(v)
                else:
                    dt[k] = [v]
        self.separate(dt)

        
            


class MapWorker:

    def __init__(self, collector, dmt = "[\W\s]"):
        self.target_collector = collector
        self.dmt = dmt

    def do_job(self, args):
        key = args[0]
        part_content = args[1]
        words = self.__pre_handle(part_content, self.dmt)
        records = dict()
        for element in words:
            element = element.lower()
            if ( element in records ):
                records[element] += 1
            else:
                records[element] = 1
        if(len(records) > 0):
            self.__send(records)    
        thread.exit()


    def __send(self, handled_data):
        self.target_collector.gather(handled_data)

    def __denoise(self, words, ptn = ""):
        for el in words:
            if re.match("[\s]", el) or el == '':
                words.remove(el)
        return words

    def __pre_handle(self, words, ptn):
        ptn = re.compile(ptn)
        raw_split = ptn.split(words)
        return self.__denoise(raw_split)

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
                    print "done!"
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

    print clt.group()