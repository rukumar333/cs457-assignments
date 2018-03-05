#!/usr/bin/env python

import glob
import sys
sys.path.append('gen-py')

from chord import FileStore
from chord import SystemException, RFileMetadata, RFile, NodeID

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

if __name__ == '__main__':
    print('Starting server')
