# Final Project - RAFT Leader Election

CIS 4307 Introduction to Distributed Systems and Networking - Spring 2020

## Overview
This Raft implementation addresses the problem of leader election in a fault tolerant asynchronous distributed system.

## Design Details
### Leader Election
The server initially starts in the follower state, if no other persisted state exists. If the server times out, then it enters the candidate state and starts an election. 

The candidate sends out RequestVoteRPCs to all of the other servers listed in the configuration file. If a majority of servers vote for the candidate, then the candidate will become leader. If the candidate discovers a server with a higher term, it becomes a follower of that server. If the candidate encounters split-votes and does not win the election, then it remains a candidate and starts another election.

As a leader, the server will send out AppendEntryRPCs to all other servers. All servers respond to the leader If the leader discovers a server with a higher term, it becomes a follower of that server.
### Timing
I used the uniform distribution from Python's random library in order to randomly draw a time between 1 and 5 seconds for server timeouts.
### RPC Limits
The server is able to handle well-above the 10 RPC per second limit. It currently averages about 15 per second.
### Persistent Storage
The server uses the logging module to log its state information every time its state is updated. The logging module is thread-safe and ensures that the data is written out, thus making it an optimal choice for this project.
### Logging
The server also uses the logging module to log its activity information.
### Testing
I verified that the program passed the recommended testing situations:

- Initially, start with no instances of your program
- Start one instance. It should start up, become a candidate, but not become a leader
- Start a second instance, it should also become candidate, but not a leader
- Start the third instance. At this point, one of the three nodes should be elected leader (since now a majority of the 5 nodes are up, i.e., 3). The rest of the nodes should become followers.
- Start the fourth and fifth nodes. These nodes should become followers, and the previously elected leader should remain the leader.

Once all N nodes are up:

- Kill the leader. One of the remaining (N-1) nodes should be elected to leader within 5 seconds
- Kill the next leader. Continue doing this, noting that new leaders will get elected within 5 seconds
- Once you kill enough leaders that a majority of the nodes are no longer up, then no nodes should becomes leader (they will remain candidates, and keep failing elections).

## Requirements
This program is written in Python 3 and uses the [RPyC](https://rpyc.readthedocs.io/en/latest/#) library.

## How to Run
Run a server by running the following on the command-line:

```python3 raftnode.py ./config.txt 0 5001```

where the 0 indicates the server number and 5001 indicates the port number.

The configuration file specifies the node, host, and port combinations.

All server information will be written in ```/tmp/```

```/tmp/state/``` will contain the most updated state information (e.g. term) for each server.

```/tmp/debug/``` will contain the activity information for each server.
