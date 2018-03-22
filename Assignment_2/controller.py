#!/usr/bin/env python

import socket
import struct
import sys
import time

import bank_pb2

# def write_to_socket(sock):
#     transfer = bank_pb2.Transfer()
#     transfer.money = 10
#     transfer.branch_name = '127.0.0.1:9091'
#     message = bank_pb2.BranchMessage()
#     message.transfer.MergeFrom(transfer)
#     message_string = message.SerializeToString()
#     sock.sendall(struct.pack('H', len(message_string)))
#     sock.sendall(message_string)

snapshot_id = 0
sockets = []

def message_socket(sock, message):
    print('Messaging socket')
    message_string = message.SerializeToString()
    print(len(message_string))
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
            sockets.append((sock, branch.port))
            message_socket(sock, message)

def initialize_snapshot(snapshot_id):
    init = bank_pb2.InitSnapshot()
    init.snapshot_id = snapshot_id
    message = bank_pb2.BranchMessage()
    message.init_snapshot.MergeFrom(init)
    print(message.WhichOneof('branch_message'))
    print('Starting on socket: {}'.format(sockets[0][1]))
    message_socket(sockets[0][0], message)

# def transfer_money():
    
        

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Need more arguments')
        sys.exit(-1)
    else:
        print(snapshot_id)
        initialize_bank(int(sys.argv[1]), sys.argv[2])
        time.sleep(3)
        initialize_snapshot(snapshot_id)
        snapshot_id = snapshot_id + 1
        
    # sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # try:
    #     sock.connect(('127.0.0.1', 9090))
    #     write_to_socket(sock)
    #     # sock.sendall('Hello world!\n')
    #     # received = sock.recv(1024)
    # finally:
    #     sock.close()
