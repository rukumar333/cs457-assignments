#!/usr/bin/env python
import glob
import hashlib
import logging
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

class FileStoreHandler:
    def __init__(self):
        self.log = {}
        self.files = {}
        self.finger_table = []

    def writeFile(self, rFile):
        print('writeFile')
        print('Content: {}'.format(rFile.content))
        if rFile.meta.contentHash in self.files:
            self.files[rFile.meta.contentHash].version += 1
        else:
            self.files[rFile.meta.contentHash] = rFile.meta
            # next line is in case a version was sent from the client
            self.files[rFile.meta.contentHash].version = 0
        file_name = rFile.meta.owner + '_' + rFile.meta.filename
        with open(file_name, 'w') as server_file:
            server_file.write(rFile.content)

    def readFile(self, filename, owner):
        print('readFile')
        hash_input = owner + ':' + filename
        hash_input = hash_input.encode('utf-8')
        file_hash = hashlib.sha256(hash_input).hexdigest()
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
        print(self.finger_table)

    def findSucc(self, key):
        print('findSucc')
        predecessor_node = self.findPred(key)
        print('Predecessor node found:')
        print(predecessor_node)
        if predecessor_node is not None: # Current node is predeccesor
            transport = TSocket.TSocket(predecessor_node.ip, predecessor_node.port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = FileStore.Client(protocol)
            transport.open()
            owner_node = client.getNodeSucc()
            transport.close()
            return owner_node
        else: # Another node is predecessor
            owner_node = self.getNodeSucc()
            return owner_node

    def findPred(self, key):
        print('findPred')
        next_node = None
        key_num = int(key, 16)
        if self.finger_table:
            if int(self.finger_table[0].id, 16) > key_num:
                # Means current node is predecessor
                return None
            for node in self.finger_table:
                if next_node is not None and (int(node.id, 16) > key_num or \
                   int(next_node.id, 16) > int(node.id, 16)):
                    # Current node succeeds key or previous node succeeds current node
                    break
                next_node = node
            assert next_node is not None
            print('Next node port: {}'.format(next_node.port))
            transport = TSocket.TSocket(next_node.ip, next_node.port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = FileStore.Client(protocol)
            transport.open()
            predecessor_node = client.findPred(key)
            transport.close()
            if predecessor_node == None:
                return next_node
            else:
                return predecessor_node
        else:
            exception = SystemException()
            exception.message = 'Node does not have a fingertable!'
            raise exception

    def getNodeSucc(self):
        print('getNodeSucc')
        if self.finger_table:
            return self.finger_table[0]
        else:
            exception = SystemException()
            exception.message = 'Node does not have a fingertable!'
            raise exception

if __name__ == '__main__':
    port_num = 9090
    if len(sys.argv) > 1:
        port_num = int(sys.argv[1])

    orig_stdout = sys.stdout
    file_stdout = open('log_' + str(port_num) + '.txt', 'w', 0)
    sys.stdout = file_stdout

    handler = FileStoreHandler()
    processor = FileStore.Processor(handler)
    transport = TSocket.TServerSocket(port=port_num)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    print('Starting server on port: {}'.format(port_num))
    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    server.serve()
    print('Ended server')
    file_stdout.close()
