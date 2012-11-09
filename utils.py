import time
from threading import Thread,Event
import socket

class RepeatTimer(Thread):
    def __init__(self, interval, function, iterations=0, args=[], kwargs={}):
        Thread.__init__(self)
        self.interval = interval
        self.function = function
        self.iterations = iterations
        self.args = args
        self.kwargs = kwargs
        self.finished = Event()
 
    def run(self):
        count = 0
        global DOWNLOADED
        while not self.finished.is_set() and (self.iterations <= 0 or count < self.iterations):
            if not self.finished.is_set():
                time1=time.time()
                self.function(*self.args, **self.kwargs)
                time2=time.time()
                count += 1
                diff=time2-time1
            if self.interval-diff>0:
                self.finished.wait(self.interval-diff)
    def cancel(self):
        self.finished.set()
        
def copyListofDict(ListofDict):
    tmpList=[]
    for node in ListofDict:
        tmpDict={}
        for key in node.keys():
            tmpDict[key]=node[key]
        tmpList.append(tmpDict)
    return tmpList

def get_ip():
    t = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try : 
        t.connect(("gmail.com",80))
        x=(t.getsockname()[0])
    except:
        x='127.0.0.1'
    print "Ip = ", x
    t.close()
    return x