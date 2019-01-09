# Chord Distributed Hash Table

## Description
This Python program creates a distributed hash table using a Chord system. Apache thrift is used for the communication between nodes.

## Implementation
* Python 2.7 
* Apache Thrift
* thrift pip package

## Set up
* Generate gen-py using chord.thrift:
```bash
$ thrift -r -gen py chord.thrift
```

## Running
Run the server using server.sh:

```bash
$ ./server.sh 9090
```

If no port is specified, the server will try to run on port 9090. server.py has a global variable LOG_TO_FILE which will output all print statements to a log file if True. If False, then print statements will be sent to stdout.

Then to initialize each nodes fingertable, use the init python script:
```bash
$ chmod +x init
$ ./init node.txt
```

The file (node.txt) should contain a list of IP addresses and ports, in the format "<ip-address>:<port>", of all of the running nodes. If four DHT nodes are running on localhost on port 9090, 9091, 9092, and 9093, then the nodes.txt should contain:
```
localhost:9090
localhost:9091
localhost:9092
localhost:9093
```

Once the nodes are running and initialized, use client.py to test the distributed hash table:
```bash
$ python client.py
```
client.py uses the same text file containing the nodes to connect to them.
