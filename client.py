import socket
from bootstraplist import BootStrapNodeList
BUFSIZE=2048

bsn = BootStrapNodeList()
bsn.generateList('bootstrap.xml',False)
bsNodes=bsn.returnList()
if len(bsNodes)==0:
    print 'No BootStrap Nodes Available..Exiting\n'
    sys.exit(0)

def get_node_list():
    nodeList=[]
    attempno=0
    myBSNode=select_bsnode(attempno)
    while(1):
	s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
	    print 'Trying to connect with', myBSNode[1],':',myBSNode[2]
	    s.connect((myBSNode[1],myBSNode[2]))        
	    print 'Connected with',myBSNode[1],':',myBSNode[2]
	    msg='?type=client?request=zonelist?'
	    s.send(msg)
	    data=s.recv(BUFSIZE)
	    nodeList=handle_list_data(data)
	    s.send('?request=endconnection?')
	    s.close()	    
	    if nodeList is not None:		
		break
        except socket.error:
            print 'Connection Attempt Failed',attempno
        attempno+=1
        myBSNode=select_bsnode(attempno)
        if myBSNode==None:
            return None
    return nodeList

def handle_list_data(data):
    nodeList=[]
    length=0
    print "data received",data
    data=data.split('?')
    for chunk in data:
        if chunk.find('length')!=-1:
            length=int(chunk.split('=')[1])
        if chunk.find('addr')!=-1:
	    address=chunk.split('=')[1]
            nodeList.append((address.split(':')[0],address.split(':')[1]))
    if len(nodeList)!=length or length==0:
        return None
    return nodeList
    
def select_bsnode(i):
    if len(bsNodes)>i:
        return bsNodes[i]
    else:
        return None


nodeList=get_node_list()
print nodeList
