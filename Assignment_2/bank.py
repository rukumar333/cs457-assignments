#!/usr/bin/env python

import logging
import SocketServer
import socket
import struct
import sys
from threading import Thread, Lock
import time

import bank_pb2

logging.basicConfig(level=logging.DEBUG)

LOG_TO_FILE = True

bank = None

class Bank(object):
    def __init__(self, name=None):
        self.branches = []
        self.sockets = []
        self.money = 0
        self.name = name
        self.money_mutex = Lock()

    def init_branch(self, message):
        print('init_branch')
        self.money = message.balance
        print('Money set: {}'.format(self.money))
        self.branches = message.all_branches
        for branch in message.all_branches:
            print(branch.name)
            print(branch.ip)
            print(branch.port)
            # sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # sock.connect((message.ip, message.port))
            # self.sockets.push_back(sock)

    def transfer(self, message):
        print('transfer')
        self.money_mutex.acquire()
        self.money = self.money + message.money
        self.money_mutex.release()
        print('New total: {}'.format(self.money))

    def init_snapshot(self, message):
        print('snapshot_id: {}'.format(message.snapshot_id))

class BankTCPHandler(SocketServer.StreamRequestHandler):
    # def __init__(self, *args, **kwargs):
    #     SocketServer.StreamRequestHandler.__init__(self, *args, **kwargs)
    # def marker(self, message):
        
    
    def handle(self):
        # self.data = self.rfile.readline()
        # print('{} wrote: '.format(self.client_address[0]))
        # print(self.data)
        # self.wfile.write('Received data!')
        data_size = struct.unpack('H', self.rfile.read(2))[0]
        data = self.rfile.read(data_size)
        message = bank_pb2.BranchMessage()
        message.ParseFromString(data)
        message_type = message.WhichOneof('branch_message')
        print('message_type: {}'.format(message_type))
        if message_type == 'init_branch':
            bank.init_branch(message.init_branch)
        elif message_type == 'transfer':
            bank.transfer(message.transfer)
            
        # transfer = bank_pb2.Transfer()
        # transfer.ParseFromString(data)
        # print(transfer.money)
        # print(transfer.branch_name)

    

if __name__ == '__main__':
    bank = Bank()
    port = 9090
    host = socket.gethostbyname(socket.gethostname())
    name = None
    if len(sys.argv) < 3:
        print('Need more arguments')
        sys.exit(-1)
    else:
        name = sys.argv[1]
        port = int(sys.argv[2])
    if LOG_TO_FILE:
        file_stdout = open('log_{}.txt'.format(port), 'w', 0)
        sys.stdout = file_stdout
    server = SocketServer.ThreadingTCPServer((host, port), BankTCPHandler)
    server.serve_forever()

