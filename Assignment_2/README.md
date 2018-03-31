# The Chandy-Lamport Snapshot Algorithm

Language used: python2

## Branch

Contained in the python script bank.py which has a wrapper shell script branch.
Start up the branch using the wrapper shell script branch as follows:

```bash
./branch BRANCH_NAME PORT_NUM
```

The python script has a class Bank which contains the state of the bank and handles all requests.
Parts of the state that are stored are the amoount of money, snapshots, and sockets to other banks.
The server is run using SocketServer.ThreadingTCPServer and SocketServer.StreamRequestHandler is used to handle requests.
The RequestHandler forwards the requests onto Bank so Bank can handle requests properly.
The Bank keeps the local objects thread safe using Locks and spawns a separate thread for sending money when initialized by the controller. Money is sent out randomly every 0 to 5 seconds.

## Controller

Contained in the python script controller.py which has a wrapper shell script controller.
Start up the controller once the branches have been set up using the script:
```bash
./controller TOTAL_MONEY_AMOUNT BRANCHES_TXT_FILE
```

The python script is used to initialize the branches with the TOTAL_MONEY_AMOUNT split between the branches contained in BRANCHES_TXT_FILE. After initializing the branches, the controller gives the branches 5 seconds to set up. After, the controller periodically (every 0 to 10 seconds) initializes a snaphot and waits 5 seconds before requesting it. The results are printed to the console and an assertion statement is used to ensure the total matches the TOTAL_MONEY_AMOUNT.

## Example Input and Output:
Branches.txt:
```
branch0 127.0.0.1 9090
branch1 127.0.0.1 9091
branch2 127.0.0.1 9092
branch3 127.0.0.1 9093
```

Running the four branches:
```bash
./branch branch0 9090&
./branch branch1 9091&
./branch branch2 9092&
./branch branch3 9093&
```

Running the controller:
```bash
./controller 8001 branches.txt
```

Controller sample output:
```
branch0
127.0.0.1
9090
branch1
127.0.0.1
9091
branch2
127.0.0.1
9092
branch3
127.0.0.1
9093
init_snapshot
Starting snapshot on: branch3
snapshot_id: 0
branch0: 2105, branch1->branch0: 0, branch2->branch0: 0, branch3->branch0: 0,
branch1: 2006, branch0->branch1: 0, branch2->branch1: 0, branch3->branch1: 0,
branch2: 1867, branch0->branch2: 0, branch1->branch2: 0, branch3->branch2: 0,
branch3: 2023, branch0->branch3: 0, branch1->branch3: 0, branch2->branch3: 0,
Total: 8001

init_snapshot
Starting snapshot on: branch2
snapshot_id: 1
branch0: 2001, branch1->branch0: 0, branch2->branch0: 0, branch3->branch0: 0,
branch1: 1951, branch0->branch1: 0, branch2->branch1: 0, branch3->branch1: 0,
branch2: 1911, branch0->branch2: 0, branch1->branch2: 0, branch3->branch2: 0,
branch3: 2138, branch0->branch3: 0, branch1->branch3: 0, branch2->branch3: 0,
Total: 8001
```
