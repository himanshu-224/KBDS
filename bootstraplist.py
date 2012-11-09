import xml.etree.ElementTree as ET

class BootStrapNode:
    def __init__(self):
        self.idno = -1
        self.ip='127.0.0.1'
        self.port = -1
        self.numNodes = -1
        
    def initNode(self,Id,Ip,Port):
        self.initNode2(Id,Ip,Port,0)
        
    def initNode2(self,Id,Ip,Port,numnodes):
        self.idno = Id
        self.ip = Ip
        self.port = Port
        self.numNodes = numnodes
        
    def updateNumnodes(Self,numnodes):
        self.numNodes = numnodes
    
    def printNode(self):
        print 'Id   : ' , self.idno
        print 'Ip   : ' , self.ip
        print 'Port : ' , self.port
        
        
class BootStrapNodeList:
    def __init__(self):
        self.nodes = {}
        self.length = 0
        
    def generateList(self,filename,debug):
        tree = ET.parse(filename)
        root = tree.getroot()
        nodeList = root.find('nodeList')
        for node in nodeList :
            newBSnode = BootStrapNode()
            Id = int(node.find('id').text)
            Ip = node.find('ip').text
            Port = int(node.find('port').text)
            if (debug):
                print Id
                print Ip
                print Port
                
            newBSnode.initNode(Id,Ip,Port)
            if not (self.nodes.has_key(Id)):
                self.nodes[Id] = newBSnode
                self.length+=1
    
    def printList(self):
        for key in self.nodes.keys():
            self.nodes[key].printNode()
            
    def returnList(self):
        List=[]
        for key in self.nodes.keys():
            List.append((self.nodes[key].idno,self.nodes[key].ip,self.nodes[key].port))
        return List
    
if __name__ == '__main__':
    bsn = BootStrapNodeList()
    bsn.generateList('bootstrap.xml',True)
    bsn.printList()
