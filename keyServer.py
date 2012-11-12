import socket
import time
import random
import operator
from bootstraplist import BootStrapNodeList
from threading import Thread
import sys
from utils import *
from keyspace import *

NUM_PART = 256

HOST=get_ip()
PORT=random.randint(5000,65000)
BUFF_SIZE=2048
bsn = BootStrapNodeList()
bsn.generateList('bootstrap.xml',False)
bsNodes=bsn.returnList()
if len(bsNodes)==0:
    print 'No BootStrap Nodes Available..Exiting\n'
    sys.exit(0)
    
serv=socket.socket(socket.AF_INET, socket.SOCK_STREAM)

while(1):
    try:
        serv.bind(('',PORT))
        print 'Port : ',PORT
        break
    except:
        PORT=random.randint(5000,65000)
        
### Server local list
MAIN_CHUNK_LIST = []
BACKUP_CHUNK_LIST = []
DATA_STORE = {}
UPDATE_INFO = {}

### Server global list
allActiveNodes=[]
CHUNK_NODE_MAP={}
for i in xrange(256):
    CHUNK_NODE_MAP[i]=['','']
    
#Select the Zone to connect to somehow
myBSNode = ()
    
class connectionThread(Thread):
    def __init__(self,conn,addr):
        Thread.__init__(self)
        self.conn=conn
        self.addr=addr
        self.BUFF_SIZE=2048
        
    def run(self):
        global allActiveNodes
        global MAIN_CHUNK_LIST
        
        while 1:
            data=self.conn.recv(self.BUFF_SIZE)
            print data
            dataDict=handle_data(data)
            if dataDict.has_key('type') and dataDict['type'] =='keyserver':
                if dataDict.has_key('request') and dataDict['request'] == 'keyspace':
                    numKeyServer = int(dataDict['numNodes'])
                    #if len(allActiveNodes) > numKeyServer:
                    #    numKeyServer = len(allActiveNodes)
                    self.requestKeyspace(numKeyServer)
                    #print MAIN_CHUNK_LIST
                elif dataDict.has_key('request') and dataDict['request'] == 'endconnection':
                    print 'Ending connection with ',self.addr[0],':',self.addr[1]
                    self.conn.close()
                    break
                    
            elif dataDict.has_key('type') and dataDict['type'] == 'bootstrap':
                if dataDict.has_key('request') and dataDict['request'] == 'update':
                    updateChunk(dataDict['chunks'])
                    Msg = '?type=keyserver?received=yes?'
                    self.conn.send(Msg)
                
                elif dataDict.has_key('request') and dataDict['request'] == 'endconnection':
                    print 'Ending connection with ',self.addr[0],':',self.addr[1]
                    self.conn.close()
                    break
                    
            elif 'type' in dataDict.keys() and 'request' in dataDict.keys() and dataDict['type']=='client' and dataDict['request']=='keyspace':
                pass            
            elif 'request' in dataDict.keys() and dataDict['request']=='endconnection':
                print 'Ending connection with ',self.addr[0],':',self.addr[1]
                self.conn.close()
                break
                
    def requestKeyspace(self,numKeyServer):
        global MAIN_CHUNK_LIST
        global BACKUP_CHUNK_LIST
        global CHUNK_NODE_MAP
        global DATA_STORE
        global allActiveNodes
        
        requestServer = str(self.addr[0]) + ':' + str(self.addr[1])
        currentServer = HOST + ':' + str(PORT)
        if numKeyServer == 2:
            transferList = repartition(MAIN_CHUNK_LIST,2)
            Msg = '?type=keyserver?reply=keyspace?main='
            #print transferList
            for item in transferList:
                MAIN_CHUNK_LIST.remove(item)
                BACKUP_CHUNK_LIST.append(item)
                CHUNK_NODE_MAP[item][0] = requestServer
                CHUNK_NODE_MAP[item][1] = currentServer
                Msg = Msg + str(item)+':'
            Msg = Msg + '?backup='
            for chunk in MAIN_CHUNK_LIST:
                CHUNK_NODE_MAP[chunk][1] = requestServer
                Msg += str(chunk)+':'
            Msg = Msg+'?'
            self.conn.send(Msg)
            return
        else :
            transferList = repartition(MAIN_CHUNK_LIST,numKeyServer)
            Msg = '?type=keyserver?reply=keyspace?main='
            for item in transferList:
                MAIN_CHUNK_LIST.remove(item)
                CHUNK_NODE_MAP[item][0] = requestServer
                Msg = Msg + str(item) + ':'
            Msg = Msg+'?'
            transferList = repartition(BACKUP_CHUNK_LIST,numKeyServer)
            Msg = Msg+'backup='
            for item in transferList:
                BACKUP_CHUNK_LIST.remove(item)
                CHUNK_NODE_MAP[item][1] = requestServer
                Msg = Msg + str(item) + ':'
            Msg = Msg+'?'
            self.conn.send(Msg)
            return
            
            

class chunkRequestThread(Thread):
    def __init__(self,keyNode):
        Thread.__init__(self)
        self.nodeIp = keyNode['ip']
        self.nodePort = keyNode['port']
        self.BUFF_SIZE = 2048
        
    def run(self):
        attempt = 0
        print 'test'
        while (attempt < 5) :
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try :
                sock.connect((self.nodeIp, int(self.nodePort)))
                print 'ChunkRequestThread\t Connected with keyServer ' + self.nodeIp+':'+self.nodePort
                self.getChunkList(sock)
                sock.close()
                break
            except socket.error, msg :
                print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
            attempt+=1                
    
    def getChunkList(self,sock):
        global MAIN_CHUNK_LIST
        global BACKUP_CHUNK_LIST
        global CHUNK_NODE_MAP
        global DATA_STORE
        global allActiveNodes
        global UPDATE_INFO
        
        numKeyServer = len(allActiveNodes)
        Msg = '?type=keyserver?request=keyspace?numNodes='+str(numKeyServer)+'?'
        sock.send(Msg)
        data = sock.recv(self.BUFF_SIZE)
        print data
        dataDict = handle_data(data)
        ### Add how to handle the list received
        currentServer = HOST + ':' + str(PORT)
        requestServer = self.nodeIp + ':' + self.nodePort
        if dataDict.has_key('type') and dataDict['type'] == 'keyserver':
            if dataDict.has_key('reply') and dataDict['reply'] == 'keyspace':
                msg = dataDict['main']
                if msg.find(':')!=-1:
                    chunklist = msg.split(':')
                    for chunks in chunklist :
                        if len(chunks)!=0:
                            chunks = int(chunks)
                            MAIN_CHUNK_LIST.append(chunks)
                            CHUNK_NODE_MAP[chunks][0] = currentServer
                            if not UPDATE_INFO.has_key(chunks):
                                UPDATE_INFO[chunks] = {}
                            elif UPDATE_INFO[chunks]['type'] == 'backup':
                                chunks += NUM_PART
                            UPDATE_INFO[chunks]['type'] = 'main'
                            UPDATE_INFO[chunks]['new'] = currentServer
                            UPDATE_INFO[chunks]['old'] = requestServer
                msg = dataDict['backup']
                if msg.find(':')!=-1:
                    chunklist = msg.split(':')
                    for chunks in chunklist :
                        if len(chunks)!=0:
                            chunks = int(chunks)
                            BACKUP_CHUNK_LIST.append(chunks)
                            CHUNK_NODE_MAP[chunks][1] = currentServer
                            if not UPDATE_INFO.has_key(chunks):
                                UPDATE_INFO[chunks] = {}
                            elif UPDATE_INFO[chunks]['type'] == 'main':
                                chunks +=NUM_PART
                            UPDATE_INFO[chunks]['type'] = 'backup'
                            UPDATE_INFO[chunks]['new'] = currentServer
                            UPDATE_INFO[chunks]['old'] = requestServer
        Msg = '?type=keyserver?request=endconnection?'
        sock.send(Msg)
        #print MAIN_CHUNK_LIST
        return

def handle_data(data):
    msgs=data.split('?')
    dataDict={}
    for msg in msgs:
        if msg.find('=')!=-1:
            dataDict[msg.split('=')[0]]=msg.split('=')[1]

    return dataDict
    
bsNodesList=[]

def updateChunk(chunkstr):
    if chunkstr.find('#') == -1:
        return
    chunklist = chunkstr.split('#')
    for chunk in chunklist:
        if len(chunk)!= 0 and chunk.find('@')!= -1:
            chunk2 = chunk.split('@')
            if chunk2[1] == 'main':
                if CHUNK_NODE_MAP[int(chunk2[0])][0] == chunk2[3]:
                    CHUNK_NODE_MAP[int(chunk2[0])][0] = chunk2[2]
            elif chunk2[1] == 'backup':
                if CHUNK_NODE_MAP[int(chunk2[0])][1] == chunk2[3]:
                    CHUNK_NODE_MAP[int(chunk2[0])][1] = chunk2[2]
    return

def initChunk(chunkstr):
    if chunkstr.find('#') == -1:
        return
    chunklist = chunkstr.split('#')
    for chunk in chunklist:
        if len(chunk)!= 0 and chunk.find('@')!= -1:
            chunk2 = chunk.split('@')
            CHUNK_NODE_MAP[int(chunk2[0])][0] = chunk2[1]
            CHUNK_NODE_MAP[int(chunk2[0])][1] = chunk2[2]
    return

def select_bsnode_sequentially(i):
    if len(bsNodes)>i:
        return bsNodes[i]
    else:
        return None
        
def select_bsnode(i):
	sorted_l=sorted(bsNodesList,key=operator.itemgetter(0))
	if len(sorted_l)>i:
		return sorted_l[i]
	else:
		return None	
	        
        
def establish_keyspace(s):    
    print 'Successfully Registered Keyspace'
    return True

###Sends update information    
def sendUpdateInfo():
    global UPDATE_INFO
    global myBSNode
    global BUFF_SIZE

    keys = UPDATE_INFO.keys()
    initMsg='?type=keyserver?request=update?continue='
    Msg = '?chunks='
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((myBSNode[1],int(myBSNode[2])))
        print 'Update part : Connected with bootstrap node ',myBSNode[1],':',myBSNode[2]
        count = 0
        for key in keys:
            if count == 60:
                Msg = initMsg+'yes'+Msg+'?'
                sock.send(Msg)
                data = sock.recv(BUFF_SIZE)
                Msg = '?chunks='
                count = 0
            if key >= NUM_PART:
                key2 = key - NUM_PART
            else:
                key2 = key
            Msg+=str(key2)+'@'+UPDATE_INFO[key]['type']+'@'+UPDATE_INFO[key]['new']+'@'+UPDATE_INFO[key]['old']+'#'
            count+=1
            del(UPDATE_INFO[key])
        Msg = initMsg+'no'+Msg+'?'
        sock.send(Msg)
        data = sock.recv(BUFF_SIZE)
        sock.close()
        UPDATE_INFO={}
    except socket.error, msg :
        print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
        
    return
        
    


def handle_list_data(data):
    nodeList=[]
    length=0
    data=data.split('?')
    for chunk in data:
        if chunk.find('length')!=-1:
            length=int(chunk.split('=')[1])
        if chunk.find('info')!=-1:
	    address=chunk.split('=')[1]
	    nodeList.append((int(address.split(':')[0]),address.split(':')[1],address.split(':')[2]))
    if len(nodeList)!=length or length==0:
        return None
    return nodeList

attemptno=0    
while(1):
    myBSNode=select_bsnode_sequentially(attemptno)
    if myBSNode==None:
        print 'Could not receive bootstrap list from any bootStrapNode node...Exiting'
        sys.exit()	    	
        
    s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
	print 'trying to connect with ',myBSNode[1],':',myBSNode[2]
	s.connect((myBSNode[1],myBSNode[2]))    
	print 'Connected with BootStrapNode ',myBSNode[1],':',myBSNode[2]
	initMsg='?type=keyserver?request=allbsinfo?'		
        s.send(initMsg)
	data=s.recv(BUFF_SIZE)
        bsNodesList=handle_list_data(data)
	print bsNodesList
        s.send('?request=endconnection?')
        s.close()
	if bsNodesList!=None:
	  break
    except socket.error:
	pass
    attemptno+=1   
                
	
attemptno=0
#allActiveNodes=[]

while(1):
    myBSNode=select_bsnode(attemptno)
    s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if myBSNode==None:
        print 'Could not register with any bootStrapNode node...Exiting'
        sys.exit()	
    try:	
        s.connect((myBSNode[1],int(myBSNode[2])))    
        print 'Connected with BootStrapNode ',myBSNode[1],':',myBSNode[2]
        initMsg='?type=keyserver?request=register?ip='+HOST+'?port='+str(PORT)+'?'
        s.send(initMsg)
        data=s.recv(BUFF_SIZE)
        dataDict=handle_data(data)
        print dataDict
        if 'reply' in dataDict.keys() and dataDict['reply']=='yes':
            print 'Initializing registeration process with ',myBSNode[1],':',myBSNode[2]
            success=establish_keyspace(s)
            addrList= dataDict['addr']
            addrList=addrList.split('#')
            for node in addrList:
                if node!='':
                    tmpDict={'ip' : node.split(':')[0], 'port' : node.split(':')[1]}
                    allActiveNodes.append(tmpDict)
            initMsg = '?type=keyserver?received=yes?'
            s.send(initMsg)
            while(1):
                data = s.recv(BUFF_SIZE)
                dataDict=handle_data(data)
                s.send(initMsg)
                if dataDict.has_key('chunks') :
                    initChunk(dataDict['chunks'])
                if dataDict.has_key('continue') and dataDict['continue']=='no':
                    s.close()
                    break
            if success==True:
                break
    except socket.error, msg:
        print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
    attemptno+=1        

print allActiveNodes
print CHUNK_NODE_MAP
#print myBSNode
### Add functionality to get all the data and chunks
if len(allActiveNodes) == 1:
    currentServer = HOST + ':' + str(PORT)
    for i in xrange(NUM_PART):
        MAIN_CHUNK_LIST.append(i)
        CHUNK_NODE_MAP[i][0] = currentServer
        CHUNK_NODE_MAP[i][1] = currentServer
        UPDATE_INFO[i] = {}
        UPDATE_INFO[i]['type']='main'
        UPDATE_INFO[i]['new']=currentServer
        UPDATE_INFO[i]['old']=''
        UPDATE_INFO[i+NUM_PART]={}
        UPDATE_INFO[i+NUM_PART]['type']='backup'
        UPDATE_INFO[i+NUM_PART]['new']=currentServer
        UPDATE_INFO[i+NUM_PART]['old']=''
else :
    #print 'else'
    currentServer = HOST + ':' + str(PORT)
    threadlist=[]
    for keyNode in allActiveNodes:
        otherServer = keyNode['ip']+':'+keyNode['port']
        #print otherServer
        if currentServer != otherServer:
            #print 'else2'
            thread = chunkRequestThread(keyNode)
            threadlist.append(thread)
            thread.start()
    for thread in threadlist:
        while thread.is_alive():
            time.sleep(0.5)
#print MAIN_CHUNK_LIST
sendUpdateInfo()

serv.listen(10) 

while(1):
    conn,addr = serv.accept()
    thread=connectionThread(conn,addr)
    thread.start()
