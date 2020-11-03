Programming language used: Python3
Tools used: Apache Thrift for compiling the provided chord.thrift file and generating the client and processor stubs along with the ttypes(types required as mentioned in chord.thrift)

How to compile:
Since I am using python3, no need to compile the code separately. Can be run directly.

How to run:
python3 src/server.py <portNumber> &

Example(for a 3 node setup):
python3 src/server/py 9090 &
python3 src/server.py 9091 &
python3 src/server.py 9092 &

Completion Status:
1. Impleneted all the expected 6 methods in full
2. Testing:
	a. Tested for getNodeSucc for a 4 node setup.
	b. Tested findSucc and findPred for a 4 node setup. Tested with each node with key as nodeid of each node and also some other keys. 
	c. Tested setFingerTable by calling init python script provided.
	d. Tested getNode,findSucc,findPred methods for the exception case where no finger table is present.
	e. Tested writeFile and readFile using another python script(not included here). Tested for the exception cases too.

NOTE: Including the FileStoreReadWriteTest.py file(which is used for writeFile and readFile tests). Please ignore this for grading purposes.
