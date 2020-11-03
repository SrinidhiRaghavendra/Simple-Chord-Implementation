import sys
import glob
sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/cs557-inst/thrift-0.13.0/lib/py/build/lib*')[0])

from chord import FileStore
from chord.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

import hashlib

def main():
    # Make socket
    transport = TSocket.TSocket(sys.argv[1], int(sys.argv[2]))

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    client = FileStore.Client(protocol)

    # Connect!
    transport.open()
    
    sha256 = hashlib.sha256()
    sha256.update(("test.txt").encode('utf-8'))
    filenamekey = sha256.hexdigest()

    node = client.findSucc(filenamekey)
    print("Found node for the test.txt file ", filenamekey, node)
    transport.close()

    rFileMeta = RFileMetadata("test.txt",0)
    rFile = RFile(rFileMeta, "This is a test file.")
    transport = TSocket.TSocket(node.ip, node.port)

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    client = FileStore.Client(protocol)

    transport.open()
    client.writeFile(rFile)
    print("Write successful")
    returnedRFile = client.readFile("test.txt")
    print("Read successful: ", returnedRFile.meta.version, returnedRFile.meta.filename, returnedRFile.content)
    # Connect!
    transport.close()

    transport = TSocket.TSocket(node.ip, 9090)

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    client = FileStore.Client(protocol)

    transport.open()
    returnedRFile = client.readFile("test.txt")
    print("Read successful: ", returnedRFile.meta.version, returnedRFile.meta.filename, returnedRFile.content)
    transport.close()

if __name__ == '__main__':
    try:
        main()
    except Thrift.TException as tx:
        print('%s' % tx.message)
