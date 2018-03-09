#!/usr/bin/zsh

./server.sh 9090&
./server.sh 9091&
./server.sh 9092&
./server.sh 9093&
python init node.txt
