#!/usr/bin/zsh

python server.py 9090&
python server.py 9091&
python server.py 9092&
python server.py 9093&
python init node.txt
