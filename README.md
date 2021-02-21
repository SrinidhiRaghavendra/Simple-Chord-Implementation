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
