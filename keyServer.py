import socket
import random
import operator
from bootstraplist import BootStrapNodeList
from threading import Thread
import sys
from utils import *

HOST=get_ip()
PORT=random.randint(5000,65000)
BUFSIZE=2048
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

### Server global list
allActiveNodes=[]
CHUNK_NODE_MAP={}        
    
#Select the Zone to connect to somehow
    
class connectionThread(Thread):
    def __init__(self,conn,addr):
        Thread.__init__(self)
        self.conn=conn
        self.addr=addr
        self.BUFF_SIZE=2048
        
    def run(self):
        while 1:
            data=self.conn.recv(self.BUF_SIZE)
            dataDict=handle_data(data)
            if dataDict.has_key('type') and dataDict['type'] =='keyserver':
                if dataDict.has_key('request') and dataDict['request'] == 'keyspace':
                    numKeyServer = int(dataDict['numNodes'])
                    #if len(allActiveNodes) > numKeyServer:
                    #    numKeyServer = len(allActiveNodes)
                    self.requestKeyspace(numKeyServer)
                elif dataDict.has_key('request') and dataDict['request'] == 'endconnection':
                    print 'Ending connection with ',self.addr[0],':',self.addr[1]
                    self.conn.close()
                    break
                    
            if 'type' in dataDict.keys() and 'request' in dataDict.key() and dataDict['type']=='client' and dataDict['request']=='keyspace':
                pass            
            elif request in dataDict.keys() and dataDict['request']=='endconnection':
                print 'Ending connection with ',self.addr[0],':',self.addr[1]
                self.conn.close()
                break
                
    def requestKeyspace(self,numKeyServer):
        requestServer = str(self.addr[0]) + ':' + str(self.addr[1])
        currentServer = HOST + ':' + str(PORT)
        if numKeyServer == 2:
            transferList = repartition(MAIN_CHUNK_LIST,2)
            Msg = '?type=keyserver?reply=keyspace?main='
            for item in transferList:
                MAIN_CHUNK_LIST.remove(item)
                BACKUP_CHUNK_LIST.append(item)
                CHUNK_NODE_MAP[item][0] = requestServer
                CHUNK_NODE_MAP[item][1] = currentServer
                Msg = Msg + str(item)+':'
            Msg = Msg + '?backup='
            for chunk in MAIN_CHUNK_LIST:
                CHUNK_NODE_MAP[item][1] = requestServer
                Msg += str(item)+':'
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
        self.nodeIp = keyNode['ip']
        self.nodePort = keyNode['port']
        self.BUFF_SIZE = 2048
        
    def run(self):
        attempt = 0
        while (attempt < 5) :
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try :
                sock.connect((self.nodeIp, int(self.nodePort)))
                print 'ChunkRequestThread\t Connected with keyServer ' + self.nodeIp+':'+self.nodePort
                self.getChunkList(sock)
                sock.close()
            except socket.error, msg :
                print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
            attempt+=1                
    
    def getChunkList(self,sock):
        numKeyServer = len(allActiveNodes) + 1
        Msg = '?type=keyserver?request=keyspace?numNodes='+str(numKeyServer)+'?'
        sock.send(Msg)
        data = sock.recv(self.BUFF_SIZE)
        dataDict = handle_data(data)
        ### Add how to handle the list received
        currentServer = HOST + ':' + str(PORT)
        requestServer = self.nodeIp + ':' + self.nodePort
        if dataDict.has_key('type') and dataDict['type'] = 'keyserver':
            if dataDict.has_key('reply') and dataDict['reply'] = 'keyspace':
                msg = dataDict['main']
                if msg.find(':')!=-1:
                    chunklist = msg.split(':')
                    for chunks in chunklist :
                        if len(chunks)!=0:
                            MAIN_CHUNK_LIST.append(int(chunks))
                            CHUNK_NODE_MAP[int(chunks)][0] = currentServer
                msg = dataDict['backup']
                if msg.find(':')!=-1:
                    chunklist = msg.split(':')
                    for chunks in chunklist :
                        if len(chunks)!=0:
                            BACKUP_CHUNK_LIST.append(int(chunks))
                            CHUNK_NODE_MAP[int(chunks)][1] = currentServer
        Msg = '?type=keyserver?request=endconnection?'
        sock.send(Msg)
        return

def handle_data(data):
    msgs=data.split('?')
    dataDict={}
    for msg in msgs:
        if msg.find('=')!=-1:
            dataDict[msg.split('=')[0]]=msg.split('=')[1]

    return dataDict
    
bsNodesList=[]

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
	data=s.recv(BUFSIZE)
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
allActiveNodes=[]

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
        data=s.recv(BUFSIZE)
        dataDict=handle_data(data)
        print dataDict
        if 'reply' in dataDict.keys() and dataDict['reply']=='yes':
            print 'Initializing registeration process with ',myBSNode[1],':',myBSNode[2]
            s.send('?request=endconnection?')
            s.close()            
            success=establish_keyspace(s)
            addrList= dataDict['addr']
            addrList=addrList.split('#')
            for node in addrList:
                if node!='':
                    tmpDict={'ip' : node.split(':')[0], 'port' : node.split(':')[1]}
                    allActiveNodes.append(tmpDict)
                    
            if success==True:
                break
    except socket.error, msg:
        print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
    attemptno+=1        

print allActiveNodes    

### Add functionality to get all the data and chunks

serv.listen(10) 

while(1):
    conn,data = serv.accept()
    thread=connectionThread(conn,addr)
    thread.start()
