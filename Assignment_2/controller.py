#!/usr/bin/env python

import random
import socket
import struct
import sys
import time

import bank_pb2

# snapshot_id = 0
sockets = []

def get_branch_index(name):
    for i in range(len(sockets)):
        if name == sockets[i][1]:
            return i

def message_socket(sock, message):
    message_string = message.SerializeToString()
    sock.sendall(struct.pack('H', len(message_string)))
    sock.sendall(message_string)

def initialize_bank(money, branches_file):
    with open(branches_file, 'r') as f:
        branches = [line.split() for line in f]
        branch_money = money / len(branches)
        init = bank_pb2.InitBranch()
        init.balance = branch_money
        for branch_arr in branches:
            branch = init.all_branches.add()
            branch.name = branch_arr[0]
            branch.ip = branch_arr[1]
            branch.port = int(branch_arr[2])
        message = bank_pb2.BranchMessage()
        message.init_branch.MergeFrom(init)
        for branch in message.init_branch.all_branches:
            print(branch.name)
            print(branch.ip)
            print(branch.port)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((branch.ip, branch.port))
            sockets.append((sock, branch.name))
            message_socket(sock, message)

def initialize_sockets(branches_file):
    with open(branches_file, 'r') as f:
        branches = [line.split() for line in f]
        for branch_arr in branches:
            name = branch_arr[0]
            ip = branch_arr[1]
            port = int(branch_arr[2])
            print(name)
            print(ip)
            print(port)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((ip, port))
            sockets.append((sock, name))

def initialize_snapshot(snapshot_id, sock):
    init = bank_pb2.InitSnapshot()
    init.snapshot_id = snapshot_id
    message = bank_pb2.BranchMessage()
    message.init_snapshot.MergeFrom(init)
    print(message.WhichOneof('branch_message'))
    print('Starting snapshot on: {}'.format(sock[1]))
    message_socket(sock[0], message)

def get_snapshot(snapshot_id, connections, money):
    snapshots = []
    retrieve = bank_pb2.RetrieveSnapshot()
    retrieve.snapshot_id = snapshot_id
    message = bank_pb2.BranchMessage()
    message.retrieve_snapshot.MergeFrom(retrieve)
    total = 0
    print('snapshot_id: {}'.format(snapshot_id))
    for sock in connections:
        result_str = ''
        # Message server
        message_socket(sock[0], message)
        # Read returned snapshot
        initial_data = sock[0].recv(2)
        data_size = struct.unpack('H', initial_data)[0]
        data = sock[0].recv(data_size)
        rec_message = bank_pb2.BranchMessage()
        rec_message.ParseFromString(data)
        assert rec_message.WhichOneof('branch_message') == 'return_snapshot'
        local_snapshot = rec_message.return_snapshot.local_snapshot
        # Get initial balance
        result_str += '{}: {}, '.format(sock[1], local_snapshot.balance)
        # if not all(x == 0 for x in local_snapshot.channel_state):
        #     print('channel_states: ')
        #     print(local_snapshot.channel_state)
        total += local_snapshot.balance
        for i in range(len(local_snapshot.channel_state)):
            if i != get_branch_index(sock[1]):
                amt = local_snapshot.channel_state[i]
                total += amt
                result_str += '{}->{}: {}, '.format(connections[i][1], sock[1], amt)
        print(result_str)
    print('Total: {}'.format(total))
    print('')
    assert total == money

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Need more arguments')
        sys.exit(-1)
    else:
        snapshot_id = 0
        init_amt = int(sys.argv[1])
        initialize_bank(init_amt, sys.argv[2])
        time.sleep(5)
        while True:
            time.sleep(random.uniform(0, 10))
            socket_index = random.randint(0, len(sockets) - 1)
            initialize_snapshot(snapshot_id, sockets[socket_index])
            time.sleep(5)
            get_snapshot(snapshot_id, sockets, init_amt)
            snapshot_id += 1
        # offset = 0
        # consec_reqs = 100
        # print(snapshot_id)
        # init_amt = int(sys.argv[1])
        # initialize_bank(init_amt, sys.argv[2])
        # # initialize_sockets(sys.argv[2])
        # while offset < 10000:
        #     time.sleep(5)
        #     for x in range(consec_reqs):
        #         initialize_snapshot(x + offset)
        #     time.sleep(25)
        #     for x in range(consec_reqs):
        #         get_snapshot(x + offset, sockets, init_amt)
        #     offset += consec_reqs
        # while snapshot_id < 50:
        #     initialize_snapshot(snapshot_id)
        #     time.sleep(3)
        #     get_snapshot(snapshot_id, sockets)
        #     snapshot_id = snapshot_id + 1
