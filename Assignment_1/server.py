#!/usr/bin/env python

import glob
import hashlib
# from pathlib import Path
import os
import sys
sys.path.append('gen-py')
# sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages/')[0])

from chord import FileStore
from chord.ttypes import SystemException, RFileMetadata, RFile, NodeID

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

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
        # server_file = Path(rFile.meta.owner + '_' + rFile.meta.filename)
        # server_file.write_text(rFile.content)

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
        # print('Inputed finger table:')
        # print(node_list)

    def findSucc(self, key):
        print('findSucc')

    def findPref(self, key):
        print('findPref')

    def getNodeSucc(self):
        print('getNodeSucc')

if __name__ == '__main__':
    port_num = 9090
    if len(sys.argv) > 1:
        port_num = int(sys.argv[1])
    handler = FileStoreHandler()
    processor = FileStore.Processor(handler)
    transport = TSocket.TServerSocket(port=port_num)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    print('Starting server on port: {}'.format(port_num))
    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    server.serve()
    print('Ended server')
