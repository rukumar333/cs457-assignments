#!/usr/bin/env python

import glob
import hashlib
import logging
import sys
sys.path.append('gen-py')
remote_packages = glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages/')
if len(remote_packages) > 0:
    sys.path.insert(0, remote_packages[0])
# sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages/')[0])

from chord import FileStore
from chord.ttypes import SystemException, RFileMetadata, RFile, NodeID

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

logging.basicConfig(level=logging.DEBUG)

def test_writeFile(list_clients):
    rFile_a = RFile()
    rFile_a.content = 'rFile_a content!'
    rFile_a.meta = RFileMetadata()
    rFile_a.meta.filename = 'rFile_a.txt'
    rFile_a.meta.owner = 'a'
    rFile_a.meta.contentHash = hashlib.sha256(rFile_a.meta.owner + ':' + \
                                              rFile_a.meta.filename).hexdigest()
    owner_node = list_clients[0][0].findSucc(rFile_a.meta.contentHash)
    print('File hash: {}'.format(rFile_a.meta.contentHash))
    print('Owner node:')
    print(owner_node)
    for client in list_clients:
        print('Client hash:')
        print(client[2])
        if owner_node.id == client[2]:
            client[0].writeFile(rFile_a)
        else:
            try:
                client[0].writeFile(rFile_a)
                assert False
            except SystemException as e:
                print(e)

    rFile_b = RFile()
    rFile_b.content = 'rFile_b content!'
    rFile_b.meta = RFileMetadata()
    rFile_b.meta.filename = 'rFile_b.txt'
    rFile_b.meta.owner = 'b'
    rFile_b.meta.contentHash = hashlib.sha256(rFile_b.meta.owner + ':' + \
                                              rFile_b.meta.filename).hexdigest()
    owner_node = list_clients[0][0].findSucc(rFile_b.meta.contentHash)
    print('File hash: {}'.format(rFile_b.meta.contentHash))
    print('Owner node:')
    print(owner_node)
    for client in list_clients:
        print('Client hash:')
        print(client[2])
        if owner_node.id == client[2]:
            client[0].writeFile(rFile_b)
        else:
            try:
                client[0].writeFile(rFile_b)
                assert False
            except SystemException as e:
                print(e)
    # for owner_client in list_clients:
    #     new_contentHash = int(owner_client[2], 16)
    #     rFile_a.meta.contentHash = new_contentHash
    #     for client in list_clients:

def test_readFile(list_clients):
    rFile_a = RFile()
    rFile_a.content = 'rFile_a content!'
    rFile_a.meta = RFileMetadata()
    rFile_a.meta.filename = 'rFile_a.txt'
    rFile_a.meta.owner = 'a'
    rFile_a.meta.contentHash = hashlib.sha256(rFile_a.meta.owner + ':' + \
                                              rFile_a.meta.filename).hexdigest()
    rFile_b = RFile()
    rFile_b.content = 'rFile_b content!'
    rFile_b.meta = RFileMetadata()
    rFile_b.meta.filename = 'rFile_b.txt'
    rFile_b.meta.owner = 'b'
    rFile_b.meta.contentHash = hashlib.sha256(rFile_b.meta.owner + ':' + \
                                              rFile_b.meta.filename).hexdigest()

    # rFile_a test
    owner_node = list_clients[0][0].findSucc(rFile_a.meta.contentHash)
    count = 0
    for client in list_clients:
        if client[2] == owner_node.id:
            count = count + 1
            client[0].writeFile(rFile_a)
    assert count == 1

    for client in list_clients:
        if owner_node.id == client[2]:
            count = count + 1
            returned_file = client[0].readFile(rFile_a.meta.filename, rFile_a.meta.owner)
            assert returned_file.content == rFile_a.content
            assert returned_file.meta.filename == rFile_a.meta.filename
            assert returned_file.meta.owner == rFile_a.meta.owner
            assert returned_file.meta.contentHash == rFile_a.meta.contentHash
            assert returned_file.meta.version == 1
        else:
            try:
                returned_file = client[0].readFile(rFile_a.meta.filename, rFile_a.meta.owner)
                assert False
            except SystemException as e:
                print(e)
    assert count == 2
    rFile_a.content = 'Updated rFile_a content!'
    for client in list_clients:
        if client[2] == owner_node.id:
            client[0].writeFile(rFile_a)
    for client in list_clients:
        if owner_node.id == client[2]:
            count = count + 1
            returned_file = client[0].readFile(rFile_a.meta.filename, rFile_a.meta.owner)
            assert returned_file.content == 'Updated rFile_a content!'
            assert returned_file.meta.filename == rFile_a.meta.filename
            assert returned_file.meta.owner == rFile_a.meta.owner
            assert returned_file.meta.contentHash == rFile_a.meta.contentHash
            assert returned_file.meta.version == 2
        else:
            try:
                returned_file = client[0].readFile(rFile_a.meta.filename, rFile_a.meta.owner)
                assert False
            except SystemException as e:
                print(e)
    assert count == 3

    # rFile_b test
    owner_node = list_clients[0][0].findSucc(rFile_b.meta.contentHash)
    count = 0
    for client in list_clients:
        if client[2] == owner_node.id:
            count = count + 1
            client[0].writeFile(rFile_b)
    assert count == 1

    for client in list_clients:
        if owner_node.id == client[2]:
            count = count + 1
            returned_file = client[0].readFile(rFile_b.meta.filename, rFile_b.meta.owner)
            assert returned_file.content == rFile_b.content
            assert returned_file.meta.filename == rFile_b.meta.filename
            assert returned_file.meta.owner == rFile_b.meta.owner
            assert returned_file.meta.contentHash == rFile_b.meta.contentHash
            assert returned_file.meta.version == 1
        else:
            try:
                returned_file = client[0].readFile(rFile_b.meta.filename, rFile_b.meta.owner)
                assert False
            except SystemException as e:
                print(e)
    assert count == 2
    rFile_b.content = 'Updated rFile_b content!'
    for client in list_clients:
        if client[2] == owner_node.id:
            client[0].writeFile(rFile_b)
    for client in list_clients:
        if owner_node.id == client[2]:
            count = count + 1
            returned_file = client[0].readFile(rFile_b.meta.filename, rFile_b.meta.owner)
            assert returned_file.content == 'Updated rFile_b content!'
            assert returned_file.meta.filename == rFile_b.meta.filename
            assert returned_file.meta.owner == rFile_b.meta.owner
            assert returned_file.meta.contentHash == rFile_b.meta.contentHash
            assert returned_file.meta.version == 2
        else:
            try:
                returned_file = client[0].readFile(rFile_b.meta.filename, rFile_b.meta.owner)
                assert False
            except SystemException as e:
                print(e)
    assert count == 3

def test_findSucc(list_clients):
    for owner_client in list_clients:
        contentHash = format((int(owner_client[2], 16) - 1), 'x')
        # print(owner_client[2])
        # print(contentHash)
        for client in list_clients:
            owner_node = client[0].findSucc(contentHash)
            assert owner_client[2] == owner_node.id

def test_findPred(list_clients):
    for pred_client in list_clients:
        contentHash = format((int(pred_client[2], 16) + 1), 'x')
        # print(pred_client[2])
        # print(contentHash)
        for client in list_clients:
            pred_node = client[0].findPred(contentHash)
            assert pred_client[2] == pred_node.id

def test_getNodeSucc(list_clients):
    list_clients.sort(key=lambda client: int(client[2], 16))
    for i in range(len(list_clients)):
        succ_node = list_clients[i][0].getNodeSucc()
        if i < len(list_clients) - 1:
            assert succ_node.id == list_clients[i + 1][2]
        else:
            assert succ_node.id == list_clients[0][2]

    for client in list_clients:
        client[0].setFingertable(list())
        try:
            succ_node = client[0].getNodeSucc()
            assert False
        except SystemException as e:
            print(e)

def main():
    list_clients = []
    with open('node.txt', 'r') as nodes:
        for node in nodes:
            node = node.strip()
            node_hash = hashlib.sha256(node).hexdigest()
            split_text = node.split(':')
            host = split_text[0]
            port = int(split_text[1])
            transport = TSocket.TSocket(host, port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = FileStore.Client(protocol)
            transport.open()
            list_clients.append((client, transport, node_hash))

    test_writeFile(list_clients)
    test_readFile(list_clients)
    test_findSucc(list_clients)
    test_findPred(list_clients)
    test_getNodeSucc(list_clients)

    # # Make socket
    # transport = TSocket.TSocket('127.0.0.1', 9090)

    # # Buffering is critical. Raw sockets are very slow
    # transport = TTransport.TBufferedTransport(transport)

    # # Wrap in a protocol
    # protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # # Create a client to use the protocol encoder
    # client = FileStore.Client(protocol)

    # # Connect!
    # transport.open()

    # rFile_a = RFile()
    # rFile_a.content = 'rFile_a content!'
    # rFile_a.meta = RFileMetadata()
    # rFile_a.meta.filename = 'rFile_a.txt'
    # rFile_a.meta.owner = 'a'
    # rFile_a.meta.contentHash = hashlib.sha256(rFile_a.meta.owner + ':' + \
    #                                           rFile_a.meta.filename).hexdigest()
    # owner_node = client.findSucc(rFile_a.meta.contentHash)
    # print('Owner:')
    # print(owner_node)

    # rFile = RFile()
    # meta_data = RFileMetadata()
    # meta_data.filename = 'test.txt'
    # # meta_data.version = 0
    # meta_data.owner = 'rushil'
    # hash_input = meta_data.owner + ':' + meta_data.filename
    # hash_input = hash_input.encode('utf-8')
    # # meta_data.contentHash = hashlib.sha256(hash_input).hexdigest()
    # meta_data.contentHash = 'fef33d8355d29002c802820e9d371b9d8c3c7a14eb072ac4a5090556a4285012'
    # rFile.content = 'Hello world!'
    # rFile.meta = meta_data
    # # client.writeFile(rFile)
    # node_owner = client.findSucc(rFile.meta.contentHash)
    # print('File:')
    # print(rFile)
    # print('Node owner:')
    # print(node_owner)

    # read_rFile = client.readFile('test.txt', 'rushil')
    # print(type(read_rFile))
    # print('Read file content: {}'.format(read_rFile.content))
    # print('Version: {}'.format(read_rFile.meta.version))

    # Close!
    # transport.close()
    for client in list_clients:
        client[1].close()


if __name__ == '__main__':
    main()
