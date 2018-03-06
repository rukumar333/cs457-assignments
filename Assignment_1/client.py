#!/usr/bin/env python

import glob
import hashlib
import sys
sys.path.append('gen-py')
# sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages/')[0])

from chord import FileStore
from chord.ttypes import SystemException, RFileMetadata, RFile, NodeID

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

def main():
    # Make socket
    transport = TSocket.TSocket('localhost', 9090)

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    client = FileStore.Client(protocol)

    # Connect!
    transport.open()

    rFile = RFile()
    meta_data = RFileMetadata()
    meta_data.filename = 'test.txt'
    # meta_data.version = 0
    meta_data.owner = 'rushil'
    hash_input = meta_data.owner + ':' + meta_data.filename
    hash_input = hash_input.encode('utf-8')
    meta_data.contentHash = hashlib.sha256(hash_input).hexdigest()
    rFile.content = 'Hello world!'
    rFile.meta = meta_data
    client.writeFile(rFile)

    read_rFile = client.readFile('test.txt', 'rushil')
    print(type(read_rFile))
    print('Read file content: {}'.format(read_rFile.content))
    print('Version: {}'.format(read_rFile.meta.version))

    # Close!
    transport.close()

if __name__ == '__main__':
    main()
