import socket
import sys
import random
import time
from threading import Thread, Event
from bootstraplist import BootStrapNodeList
from utils import *

bsn = BootStrapNodeList()
bsn.generateList('bootstrap.xml',False)
bsNodes=bsn.returnList()

try :
    Id= int(sys.argv[1])
except:
    print 'Usage : python KBDSserver.py Id'
    sys.exit()

PORT=None
for node in bsNodes:
    if int(node[0])==Id:
	PORT=int(node[2])
	HOST=node[1]
	break
		
if PORT==None:
	print 'Id does not exist at bootStrapList...Exiting'
	sys.exit()

def pingBootStrapNodes():
	global bootStrapNodes
	global activeNodes
	tmpList=copyListofDict(bootStrapNodes)
	for node in bsNodes:
	    if int(node[0])!=Id:
		t = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		try:
		    t.connect((node[1],node[2]))
		    t.send('?type=bootstrap?request=bsinfo?ip='+HOST+'?port='+str(PORT)+'?numNodes='+str(len(activeNodes))+'?')
		    data=t.recv(BUF_SIZE)
		    tmpDict=handle_data(data)
		    exists=False
		    for node in tmpList:
			if node['ip']==tmpDict['ip'] and node['port']==tmpDict['port']:
			    node['numNodes']=tmpDict['numNodes']
			    exists=True
			    break
		    if exists==False:
		      tmpList.append(tmpDict)		    
		      
		    message=active_nodes_list_string(activeNodes)
		    t.send(message)
		    data=t.recv(BUF_SIZE)
		    t.send('?request=endconnection?')
		    t.close()
		except socket.error:
		    pass
	    else:
		exists=False
		for node in tmpList:
		    if node['ip']==HOST and node['port']==str(PORT):
			node['numNodes']=str(len(activeNodes))
			exists=True
			break
		if exists==False:
		    tmpList.append({'ip':HOST, 'port':str(PORT), 'numNodes':str(len(activeNodes))})
		    
	    bootStrapNodes=tmpList
	    #print tmpList
                
HOST=get_ip()
#PORT=5789
BUF_SIZE=2048

def bsinfo():
	message='?ip='+str(HOST)+'?port='+str(PORT)+'?numNodes='+str(len(activeNodes))+'?'
	return message

class connectionThread(Thread):
    def __init__(self,conn,addr):
        Thread.__init__(self)
        self.conn=conn
        self.addr=addr
        self.BUF_SIZE=2048
        
    def run(self):
        while 1:
	    global bootStrapNodes
	    global activeNodes
	    global allActiveNodes
            data=self.conn.recv(self.BUF_SIZE)
            dataDict=handle_data(data)
            #print dataDict
           
            if 'type' in dataDict.keys() and 'request' in dataDict.keys() and dataDict['type']=='client' and dataDict['request']=='zonelist':
                message=nodes_list_string(allActiveNodes)
                self.conn.send(message)
           
	    elif 'type' in dataDict.keys() and 'request' in dataDict.keys() and 'length' in dataDict.keys() and 'addr' in dataDict.keys() and dataDict['type']=='bootstrap' and dataDict['request']=='getnodes':
		addrList= dataDict['addr']
		addrList=addrList.split('#')
		tmpAllActiveNodes=copyListofDict(allActiveNodes)
		for node in addrList:
		    if node!='':
			exists=False
			tmpDict={'ip' : node.split(':')[0], 'port' : node.split(':')[1]}
			if tmpDict not in tmpAllActiveNodes:
			    tmpAllActiveNodes.append(tmpDict)
		allActiveNodes =tmpAllActiveNodes
		self.conn.send('?noted=yes?')      
		      
		      
            elif 'type' in dataDict.keys() and 'request' in dataDict.keys() and dataDict['type']=='keyserver' and dataDict['request']=='allbsinfo':    
		message=bsnodes_list_string(bootStrapNodes)
		print bootStrapNodes
		self.conn.send(message)
            elif 'type' in dataDict.keys() and 'request' in dataDict.keys() and 'ip' in dataDict.keys() and 'port' in dataDict.keys()  and dataDict['type']=='keyserver' and dataDict['request']=='register':
                    
		tmpDict={'ip':dataDict['ip'], 'port':dataDict['port']}
		if  tmpDict not in activeNodes:
		    activeNodes.append(tmpDict)
		if tmpDict not in allActiveNodes:
		    allActiveNodes.append(tmpDict)
		message=active_nodes_list_string(allActiveNodes)
		self.conn.send('?reply=yes'+message)
		pingBootStrapNodes()
            
            elif 'type' in dataDict.keys() and 'request' in dataDict.keys() and 'numNodes' in dataDict.keys() and 'ip' in dataDict.keys() and 'port' in dataDict.keys()  and dataDict['type']=='bootstrap' and dataDict['request']=='bsinfo':    
		message=bsinfo()
		tmpDict={'ip':dataDict['ip'], 'port':dataDict['port'] , 'numNodes' : dataDict['numNodes']}
		exists=False
		for node in bootStrapNodes:
		  if node['ip']==tmpDict['ip'] and node['port']==tmpDict['port']:
		    node['numNodes']=tmpDict['numNodes']
		    exists=True
		    break
		if exists==False:
		  bootStrapNodes.append(tmpDict)
		self.conn.send(message)
				
            elif 'request' in dataDict.keys() and dataDict['request']=='endconnection':
                self.conn.close()
                print "Ended connection with ",self.addr[0],':',self.addr[1]
                break
            

            
def active_nodes_list_string(activeNodes):
    i=0
    l=len(activeNodes)
    string='?type=bootstrap?request=getnodes?length='+str(l)+'?addr='
    for node in activeNodes:
        string+=str(node['ip'])+':'+str(node['port'])+'#'
    string+='?'
    return string
    
def nodes_list_string(activeNodes):
    i=0
    l=len(activeNodes)
    string='?length='+str(l)+'?'
    for node in activeNodes:
        string+='addr'+'='+str(node['ip'])+':'+str(node['port'])+'?'
    return string

def bsnodes_list_string(bootStrapNodes):
    i=0
    l=len(bootStrapNodes)
    string='?length='+str(l)+'?'
    for node in bootStrapNodes:
        string+='info'+'='+str(node['numNodes'])+':'+str(node['ip'])+':'+str(node['port'])+'?'
    return string

def handle_data(data):
    msgs=data.split('?')
    dataDict={}
    for msg in msgs:
        if msg.find('=')!=-1:
            dataDict[msg.split('=')[0]]=msg.split('=')[1]
    return dataDict
    
activeNodes=[]
allActiveNodes=[]
bootStrapNodes=[{'ip':HOST, 'port':str(PORT), 'numNodes':str(len(activeNodes))}]

#Initial Phase : get information on current load from each bootstrap Node

#t=5.0
#pingbsThread=RepeatTimer(t,pingBootStrapNodes)
#pingbsThread.start()

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

try:
    s.bind(('', PORT))
except socket.error , msg:
    print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
    sys.exit()

s.listen(10)


while 1:
    conn,addr=s.accept()
    print "Created connection with ",addr[0],':',addr[1]
    thread=connectionThread(conn,addr)
    thread.start()
