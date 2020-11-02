import glob
import sys
sys.path.append('../gen-py')
sys.path.insert(0, glob.glob('/home/cs557-inst/thrift-0.13.0/lib/py/build/lib*')[0])

from chord import FileStore
from chord import ttypes

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

import hashlib
import socket

class ProcessorHandler:
    def __init__(self, port):
        self.files = {}
        self.fingerTable = None
        self.pred = None
        self.ipaddr = socket.gethostbyname(socket.gethostname()) 
        self.port = port
        sha256 = hashlib.sha256()
        sha256.update((self.ipaddr+":"+self.port).encode('utf-8'))
        self.id = sha256.hexdigest()
        self.node = ttypes.NodeID(self.id, self.ipaddr, self.port)

    def writeFile(self, rFile):
        rFileMeta = rFile.meta
        filename = rFileMeta.filename
        sha256 = hashlib.sha256()
        sha256.update(filename.encode('utf-8'))
        fileid = sha256.hexdigest()
        if(self.pred == None):
            self.pred = self.findPred(self.id)
        if(fileid <= self.pred.id or fileid > self.id):
            raise ttypes.SystemException("File id does not belong to this node")
        if(filename in self.files):
            self.files[filename] += 1
        else:
            self.files[filename] = 0

        fileWrite = open(filename, "w")
        fileWrite.write(rFile.content)
        fileWrite.flush()
        fileWrite.close()


    def readFile(self, filename):
        if(filename not in self.files or fileid <= self.pred or fileid > self.id):
            raise ttypes.SystemException("Given filename not present in this node")
        sha256 = hashlib.sha256()
        sha256.update(filename.encode('utf-8'))
        fileid = sha256.hexdigest()
        readFile = open(filename, "r")
        content = readFile.read()
        readFile.close()

        metadata = RFileMetadata(filename, self.files[filename])
        rFile = RFile(metadata, content)
        return rFile

    def setFingertable(self, node_list):
        self.fingerTable = node_list

    def findSucc(self, key):
        if(self.fingerTable is None or len(self.fingerTable) == 0):
            raise ttypes.SystemException("No finger table present for this node")
        if(key == self.id):
            return self
        pred = self.findPred(key);
        print(type(pred))
        #ask pred node for its succesor and return it
        transport = TSocket.TSocket(pred.ip, int(pred.port))
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = FileStore.Client(protocol)
        transport.open()
        succ = client.getNodeSucc()
        transport.close()
        return succ

    def findPred(self, key):
        if(self.fingerTable is None or len(self.fingerTable) == 0):
            raise ttypes.SystemException("No finger table present for this node")
        if(key <= self.id or key > self.fingerTable[0].id):
            #ask the closest known pred node
            predNode = self.closestPred(key)
            if(predNode != self):
                #ask the pred node for the actual pred node
                transport = TSocket.TSocket(predNode.ip, int(predNode.port))

                # Buffering is critical. Raw sockets are very slow
                transport = TTransport.TBufferedTransport(transport)
                # Wrap in a protocol
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                # Create a client to use the protocol encoder
                client = FileStore.Client(protocol)
                transport.open()
                pred = client.findPred(key)
                transport.close()
                return pred
            else:
                return self
        else:
            return self

    def closestPred(self, key):
        if(key < self.id):
            prev = self.node
            for i in range(0,len(self.fingerTable)):
                if(self.fingerTable[i].id > key):
                    break
                else:
                    prev = self.fingerTable[i]
            return prev
        else:
            for i in range(len(self.fingerTable)-1,-1,-1):
                if(self.fingerTable[i].id < key):
                    return self.fingerTable[i]
            return self

    def getNodeSucc(self):
        if(self.fingerTable is None or len(self.fingerTable) == 0):
            raise ttypes.SystemException("No finger table present for this node")
        return self.fingerTable[0]




if __name__ == '__main__':
    handler = ProcessorHandler(sys.argv[1])
    processor = FileStore.Processor(handler)
    transport = TSocket.TServerSocket(port=int(sys.argv[1]))
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    print('Starting the server...')
    server.serve()
    print('done.')
