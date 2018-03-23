#!/usr/bin/env python

import logging
import random
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
        print('Name: {}'.format(name))
        self.branches = []
        self.sockets = []
        self.snapshots = {}
        self.channel_states = {}
        self.balance = 0
        self.name = name
        self.balance_mutex = Lock()
        # self.snapshots_mutex = Lock()

    # Utility funcs
    def message_socket(self, sock, message):
        # print('Messaging socket')
        message_string = message.SerializeToString()
        # print(len(message_string))
        sock.sendall(struct.pack('H', len(message_string)))
        sock.sendall(message_string)

    def send_money(self):
        while True:
            time_to_sleep = int(random.uniform(0, 5))
            print('Sleeping for: {}'.format(time_to_sleep))
            time.sleep(time_to_sleep)
            self.balance_mutex.acquire()
            amount = int((random.uniform(0.01, 0.05) * self.balance))
            self.balance = self.balance - amount
            assert self.balance >= 0
            self.balance_mutex.release()
            print('Sending: {}'.format(amount))
            transfer = bank_pb2.Transfer()
            transfer.money = amount
            transfer.branch_name = self.name
            message = bank_pb2.BranchMessage()
            message.transfer.MergeFrom(transfer)
            self.sockets[2][1].acquire()
            self.message_socket(self.sockets[2][0], message)
            self.sockets[2][1].release()

    def get_branch_index(self, name):
        for i in range(len(self.branches)):
            if name == self.branches[i].name:
                return i

    # Message funcs
    def init_branch(self, message):
        print('init_branch')
        self.balance_mutex.acquire()
        self.balance = message.balance
        print('Money set: {}'.format(self.balance))
        self.branches = message.all_branches
        for branch in self.branches:
            print(branch.name)
            print(branch.ip)
            print(branch.port)
            if branch.name != self.name:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((branch.ip, branch.port))
                self.sockets.append((sock, Lock(), branch.name))
        self.balance_mutex.release()
        # if self.name == 'branch0':
        thread = Thread(target=self.send_money)
        thread.start()

    def transfer(self, message):
        print('transfer')
        # Lock to make sure no conflict with init_snapshot
        self.balance_mutex.acquire()
        self.balance = self.balance + message.money
        # Check if snapshot required listening on channels
        for _, val in self.channel_states.iteritems():
            channel_index = self.get_branch_index(message.branch_name)
            if val[channel_index] is not None: # Means channel is set to empty
                val[channel_index] += message.money
        self.balance_mutex.release()                
        print('New total: {}'.format(self.balance))

    def init_snapshot(self, message):
        print('snapshot_id: {}'.format(message.snapshot_id))
        # Set marker
        marker = bank_pb2.Marker()
        marker.branch_name = self.name
        marker.snapshot_id = message.snapshot_id
        # Create local snapshot to send later
        stored_snapshot = bank_pb2.ReturnSnapshot()
        stored_snapshot.local_snapshot.snapshot_id = message.snapshot_id
        # Lock to make sure init_snapshot does not conflict with transfer
        self.balance_mutex.acquire()
        # Set balance of local snapshot and default value channel_states
        stored_snapshot.local_snapshot.balance = self.balance
        stored_snapshot.local_snapshot.channel_state[:] = [0 for _ in range(len(self.branches))]
        print('Initial channel_state')
        print(stored_snapshot.local_snapshot.channel_state)
        # Store local snapshot which means it has seen snapshot_id
        self.snapshots[message.snapshot_id] = (stored_snapshot, True)
        self.channel_states[message.snapshot_id] = [0 for _ in range(len(self.branches))]
        self.balance_mutex.release()
        new_message = bank_pb2.BranchMessage()
        new_message.marker.MergeFrom(marker)
        for sock in self.sockets:
            sock[1].acquire()
            self.message_socket(sock[0], new_message)
            sock[1].release()

    def marker(self, message):
        print('marker')
        if message.snapshot_id in self.snapshots:
            print('Already seen')
            # Already saw snapshot_id
            # Should be nothing to do but might have to fix this
            # Need to mark channel empty

            # Copy over values from channel states to snapshot
            for i in range(len(self.channel_states[message.snapshot_id])):
                if self.channel_states[message.snapshot_id][i] is not None:
                    self.snapshots[message.snapshot_id][0]\
                        .local_snapshot.channel_state[i] = self.channel_states[message.snapshot_id][i]
            # Stop listening to channel
            channel_index = self.get_branch_index(message.branch_name)
            self.channel_states[message.snapshot_id][channel_index] = None
        else:
            # First time seeing snapshot_id
            print('snapshot_id: {}'.format(message.snapshot_id))
            print('From: {}'.format(message.branch_name))
            # Set marker
            marker = bank_pb2.Marker()
            marker.branch_name = self.name
            marker.snapshot_id = message.snapshot_id
            # Create local snapshot to send later
            stored_snapshot = bank_pb2.ReturnSnapshot()
            stored_snapshot.local_snapshot.snapshot_id = message.snapshot_id
            self.balance_mutex.acquire()
            # Set balance of local snapshot and default value channel_states
            stored_snapshot.local_snapshot.balance = self.balance
            stored_snapshot.local_snapshot.channel_state[:] = [0 for _ in range(len(self.branches))]
            print('Initial channel_state')
            print(stored_snapshot.local_snapshot.channel_state)
            # Store local snapshot which means it has seen snapshot_id
            self.snapshots[message.snapshot_id] = (stored_snapshot, True)
            self.channel_states[message.snapshot_id] = [0 for _ in range(len(self.branches))]
            incoming_channel_index = self.get_branch_index(message.branch_name)
            # Set incoming channel as empty for snapshot
            self.channel_states[message.snapshot_id][incoming_channel_index] = None
            new_message = bank_pb2.BranchMessage()
            new_message.marker.MergeFrom(marker)
            for sock in self.sockets:
                # # Make sure to not send marker
                # if sock[2] != message.branch_name:
                sock[1].acquire()
                self.message_socket(sock[0], new_message)
                sock[1].release()
            self.balance_mutex.release()

    def retrieve_snapshot(self, message, client):
        print('retrieve_snapshot')
        snapshot = self.snapshots[message.snapshot_id][0]
        new_message = bank_pb2.BranchMessage()
        new_message.return_snapshot.CopyFrom(snapshot)
        print('Messaging socket')
        message_string = new_message.SerializeToString()
        print(len(message_string))
        client.write(struct.pack('H', len(message_string)))
        client.write(message_string)
        

    def return_snapshot(self, message):
        print('return_snapshot')

class BankTCPHandler(SocketServer.StreamRequestHandler):
    # def __init__(self, *args, **kwargs):
    #     SocketServer.StreamRequestHandler.__init__(self, *args, **kwargs)
    # def marker(self, message):
        
    
    def handle(self):
        # self.data = self.rfile.readline()
        # print('{} wrote: '.format(self.client_address[0]))
        # print(self.data)
        # self.wfile.write('Received data!')
        while True:
            print('Received data')
            initial_data = self.rfile.read(2)
            if not initial_data:
                break
            data_size = struct.unpack('H', initial_data)[0]
            data = self.rfile.read(data_size)
            message = bank_pb2.BranchMessage()
            message.ParseFromString(data)
            message_type = message.WhichOneof('branch_message')
            print('message_type: {}'.format(message_type))
            if message_type == 'init_branch':
                bank.init_branch(message.init_branch)
            elif message_type == 'transfer':
                bank.transfer(message.transfer)
            elif message_type == 'init_snapshot':
                bank.init_snapshot(message.init_snapshot)
            elif message_type == 'marker':
                bank.marker(message.marker)
            elif message_type == 'retrieve_snapshot':
                bank.retrieve_snapshot(message.retrieve_snapshot, self.wfile)
        # elif message_type == 'return_snapshot':
        #     bank.return_snapshot(message.return_snapshot)
            
        # transfer = bank_pb2.Transfer()
        # transfer.ParseFromString(data)
        # print(transfer.money)
        # print(transfer.branch_name)

    

if __name__ == '__main__':
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
    bank = Bank(name)        
    server = SocketServer.ThreadingTCPServer((host, port), BankTCPHandler)
    # server = SocketServer.TCPServer((host, port), BankTCPHandler)
    server.serve_forever()

