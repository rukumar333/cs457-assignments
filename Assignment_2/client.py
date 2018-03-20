#!/usr/bin/env python

import socket
import struct
import sys

import bank_pb2

def write_to_socket(sock):
    transfer = bank_pb2.Transfer()
    transfer.money = 10
    transfer.branch_name = '127.0.0.1:9091'
    message = bank_pb2.BranchMessage()
    message.transfer.MergeFrom(transfer)
    message_string = message.SerializeToString()
    sock.sendall(struct.pack('H', len(message_string)))
    sock.sendall(message_string)

if __name__ == '__main__':
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(('127.0.0.1', 9090))
        write_to_socket(sock)
        # sock.sendall('Hello world!\n')
        # received = sock.recv(1024)
    finally:
        sock.close()
