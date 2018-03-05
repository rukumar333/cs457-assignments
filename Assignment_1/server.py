#!/usr/bin/env python

import glob
import hashlib
from pathlib import Path
import sys
sys.path.append('gen-py')

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

    def writeFile(self, rFile):
        print('writeFile')
        print('Content: {}'.format(rFile.content))
        self.files[rFile.meta.contentHash] = rFile.meta
        server_file = Path(rFile.meta.owner + '_' + rFile.meta.filename)
        if server_file.is_file():
            self.files[rFile.meta.version] += 1
        server_file.write_text(rFile.content)
            

    def readFile(self, filename, owner):
        print('readFile')
        hash_input = owner + ':' filename
        hash_input = hash_input.encode('utf-8')
        file_hash = hashlib.sha256(file_hash).hexdigest()
        if file_hash in self.files:
            rFile = RFile()
            rFile.content = Path(owner + '_' + filename).read_text()
            rFile.meta = self.files[file_hash]
            return rFile
        else:
            exception = SystemException()
            exception.message = 'File {} owned by {} does not exist!'.format(filename, owner)
            raise exception

    def setFingerTable(self, node_list):
        print('setFingerTable')

    def findSucc(self, key):
        print('findSucc')

    def findPref(self, key):
        print('findPref')

    def getNodeSucc(self):
        print('getNodeSucc')

if __name__ == '__main__':
    handler = FileStoreHandler()
    processor = FileStore.Processor(handler)
    transport = TSocket.TServerSocket(port=9090)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    print('Starting server')
    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    server.serve()
    print('Ended server')
