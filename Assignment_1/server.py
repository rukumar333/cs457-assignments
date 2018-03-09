#!/usr/bin/env python
import glob
import hashlib
import logging
import socket
import sys
sys.path.append('gen-py')
# sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages/')[0])

from chord import FileStore
from chord.ttypes import SystemException, RFileMetadata, RFile, NodeID

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

logging.basicConfig(level=logging.DEBUG)

LOG_TO_FILE = True

def is_between(begin, end, key):
    if begin < end:
        return key > begin and key <= end
    elif end < begin:
        return key > begin or key <= end
    else:
        return False

class FileStoreHandler:
    def __init__(self):
        self.log = {}
        self.files = {}
        self.finger_table = []
        self.node_id = NodeID()
        self.node_id.ip = socket.gethostbyname(socket.gethostname())

    def writeFile(self, rFile):
        print('writeFile')
        print('Content: {}'.format(rFile.content))
        if not rFile.meta.contentHash:
            hash_input = rFile.meta.owner + ':' + rFile.meta.filename
            hash_input = hash_input.encode('utf-8')
            rFile.meta.contentHash = hashlib.sha256(hash_input).hexdigest()
        owner_node = self.findSucc(rFile.meta.contentHash)
        if owner_node.id != self.node_id.id:
            exception = SystemException()
            exception.message = 'Node {}:{} does not own file {}:{}'.format(self.node_id.ip, \
                                                                            self.node_id.port, \
                                                                            rFile.meta.owner, \
                                                                            rFile.meta.filename)
            raise exception
        if rFile.meta.contentHash in self.files:
            self.files[rFile.meta.contentHash].version += 1
        else:
            self.files[rFile.meta.contentHash] = rFile.meta
            # next line is in case a version was sent from the client
            self.files[rFile.meta.contentHash].version = 0
        print('Meta data:')
        print(self.files[rFile.meta.contentHash])
        file_name = rFile.meta.owner + '_' + rFile.meta.filename
        with open(file_name, 'w') as server_file:
            server_file.write(rFile.content)

    def readFile(self, filename, owner):
        print('readFile')
        hash_input = owner + ':' + filename
        hash_input = hash_input.encode('utf-8')
        file_hash = hashlib.sha256(hash_input).hexdigest()
        owner_node = self.findSucc(file_hash)
        if owner_node.id != self.node_id.id:
            exception = SystemException()
            exception.message = 'Node {}:{} does not own file {}:{}'.format(self.node_id.ip, \
                                                                            self.node_id.port, \
                                                                            owner, \
                                                                            filename)
            raise exception
        if file_hash in self.files:
            file_name = owner + '_' + filename
            rFile = RFile()
            with open(file_name, 'r') as server_file:
                rFile.content = server_file.read()
            rFile.meta = self.files[file_hash]
            print('Returning file:')
            print(rFile)
            return rFile
        else:
            exception = SystemException()
            exception.message = 'File {} owned by {} does not exist!'.format(filename, owner)
            raise exception

    def setFingertable(self, node_list):
        print('setFingertable')
        self.finger_table = node_list
        print('Length of fingertable: {}'.format(len(node_list)))
        # print(self.finger_table)

    def findSucc(self, key):
        print('findSucc')
        predecessor_node = self.findPred(key)
        print('Predecessor node found:')
        print(predecessor_node)
        if predecessor_node is not self.node_id: # Another node is predeccesor
            assert predecessor_node.id != self.node_id.id
            transport = TSocket.TSocket(predecessor_node.ip, predecessor_node.port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = FileStore.Client(protocol)
            transport.open()
            owner_node = client.getNodeSucc()
            transport.close()
            return owner_node
        else: # Current node is predecessor
            owner_node = self.getNodeSucc()
            return owner_node

    def findPred(self, key):
        print('findPred')
        next_node = None
        key_num = int(key, 16)
        if self.finger_table:
            print('Checking if current node is predeccesor')
            if is_between(int(self.node_id.id, 16), int(self.finger_table[0].id, 16), key_num):
                # Means current node is predecessor
                # Base case
                return self.node_id
            found_predecessor = False
            print('Checking if key is between nodes in fingertable')
            for node in self.finger_table:
                if next_node is not None and is_between(int(next_node.id, 16), \
                                                        int(node.id, 16), key_num):
                    found_predecessor = True
                    break
                next_node = node
            if found_predecessor is False:
                # Node was not in between any two nodes in finger table. Need to check last node then
                print('Using last node in finger table')
                next_node = self.finger_table[-1]
            assert next_node.id != self.node_id.id
            print('Next node {}:{}'.format(next_node.ip, next_node.port))
            transport = TSocket.TSocket(next_node.ip, next_node.port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = FileStore.Client(protocol)
            transport.open()
            predecessor_node = client.findPred(key)
            print('Node returned fron next node:')
            print(predecessor_node)
            transport.close()
            return predecessor_node
        else:
            exception = SystemException()
            exception.message = 'Node {}:{} does not have a fingertable!'.format(self.node_id.ip, \
                                                                                 self.node_id.port)
            raise exception

    def getNodeSucc(self):
        print('getNodeSucc')
        if self.finger_table:
            return self.finger_table[0]
        else:
            exception = SystemException()
            exception.message = 'Node {}:{} does not have a fingertable!'.format(self.node_id.ip, \
                                                                                 self.node_id.port)
            raise exception

if __name__ == '__main__':
    port_num = 9090
    if len(sys.argv) > 1:
        port_num = int(sys.argv[1])

    handler = FileStoreHandler()
    handler.node_id.port = port_num
    hash_input = handler.node_id.ip + ':' + str(handler.node_id.port)
    hash_input = hash_input.encode('utf-8')
    handler.node_id.id = hashlib.sha256(hash_input).hexdigest()

    orig_stdout = sys.stdout
    if LOG_TO_FILE:
        file_stdout = open('log_{}_{}.txt'.format(handler.node_id.ip, handler.node_id.port), 'w', 0)
        sys.stdout = file_stdout

    print('Node info:')
    print('IP:Port {}:{}'.format(handler.node_id.ip, handler.node_id.port))
    print('ID: {}'.format(handler.node_id.id))
    processor = FileStore.Processor(handler)
    transport = TSocket.TServerSocket(port=port_num)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    print('Starting server on port: {}'.format(port_num))
    # server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    server.serve()
    print('Ended server')
    file_stdout.close()
