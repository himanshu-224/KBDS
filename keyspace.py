import random
from hashlib import sha1,md5

NUM_PART = 256

def hashAlgo(hashAlgorithm):
    if hashAlgorithm == 'SHA1' or hashAlgorithm == 'sha1' :
        return sha1()
    elif hashAlgorithm == 'MD5' or hashAlgorithm == 'md5' :
        return md5()
    else :
        raise ValueError, 'invalid hashing algorithm\n'
    
def repartition(chunklist,numServer):
    MIN_PART = NUM_PART/numServer
    transferList = []
    dicLen = len(chunklist)
    #print dicLen,MIN_PART
    diff = dicLen - MIN_PART
    if diff <=0 :
        return transferList
    counter = 0
    x = dicLen/diff
    while diff>0:
        chunkId = random.randint(0,dicLen-1)
        if chunklist[chunkId] not in transferList:
            transferList.append(chunklist[chunkId])
            diff-=1
    return transferList

def getPartitionId(key):
    a = key[0:2]
    Id = hex2decimal(a[0])*16+hex2decimal(a[1])
    return Id
    
    
def hex2decimal(x):
    if x>='0' and x<='9' :
        return int(x)
    elif x>='a' and x<'g':
        return (ord(x) - ord('a') + 10)