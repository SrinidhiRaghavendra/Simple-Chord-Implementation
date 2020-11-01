import glob
import sys
sys.path.append('../gen-py')
sys.path.insert(0, glob.glob('/home/cs557-inst/thrift-0.13.0/lib/py/build/lib*')[0])

from chord import FileStore
from chord import ttypes

from thrift.transport import TSocket
from thrift.transport import TTransport,TBufferedTransport
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
        sha256.update((ipaddr+":"+port).encode('utf-8'))
        self.id = sha256.hexdigest()
       

    def writeFile(self, rFile):
        rFileMeta = rFile.meta
        filename = rFileMeta.filename
        sha256 = hashlib.sha256()
        sha256.update(filename.encode('utf-8'))
        fileid = sha256.hexdigest()
        if(self.pred == None):
            self.pred = self.findPred(self.id)
        if(fileid <= self.pred or fileid > self.id):
            #throw system error
        if(filename in self.files):
            self.files[filename] += 1
        else:
            self.files[filename] = 0

        fileWrite = open(filename, "w")
        fileWrite.write(rFile.content)
        fileWrite.flush()
        fileWrite.close()


    def readFile(self, filename):
        if(filename not in self.files):
            #throw System error
        
        sha256 = hashlib.sha256()
        sha256.update(filename.encode('utf-8'))
        fileid = sha256.hexdigest()
        if(fileid <= self.pred or fileid > self.id):
            #throw System error
        readFile = open(filename, "r")
        content = readFile.read()
        readFile.close()

        metadata = RFileMetadata(filename, self.files[filename])
        rFile = RFile(metadata, content)
        return rFile

    def setFingertable(self, node_list):
        self.fingerTable = node_list

    def findSucc(self, key):
        if(key == self.id):
            return self
        pred = self.findPred(key);
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
        for i in range(len(self.fingerTable)-1,-1,-1):
            if(self.fingerTable[i].id > self.id and self.fingerTable[i].id < key):
                return self.fingerTable[i]
        return self

    def getNodeSucc(self):
        if(self.fingerTable is None or len(self.fingerTable) == 0):
            #throw error
        return self.fingerTable[0]




if __name__ == '__main__':
    handler = ProcessorHandler(sys.argv[1])
    processor = FileStore.Processor(handler)
    transport = TSocket.TServerSocket(port=int(sys.argv[1]))
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

    # You could do one of these for a multithreaded server
    # server = TServer.TThreadedServer(
    #     processor, transport, tfactory, pfactory)
    # server = TServer.TThreadPoolServer(
    #     processor, transport, tfactory, pfactory)

    print('Starting the server...')
    server.serve()
    print('done.')
