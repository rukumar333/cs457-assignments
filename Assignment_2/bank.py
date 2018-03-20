#!/usr/bin/env python

import SocketServer
import socket
import struct
import sys
from threading import Thread, Lock
import time

import bank_pb2

bank = None

class Bank(object):
    def __init__(self, name=None):
        self.branches = []
        self.sockets = []
        self.money = 0
        self.name = name
        self.money_mutex = Lock()

    def init_branch(self, message):
        self.money = message.balance
        self.branches = message.branches
        for branch in message.branches:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((message.ip, message.port))
            self.sockets.push_back(sock)

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
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    server = SocketServer.ThreadingTCPServer(('127.0.0.1', port), BankTCPHandler)
    server.serve_forever()
