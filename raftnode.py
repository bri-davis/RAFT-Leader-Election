import sys
import os
import rpyc
from rpyc.utils.server import ThreadPoolServer
from threading import Thread
from threading import Timer
from threading import Lock
import logging
from random import uniform
'''
A RAFT RPC server class.

Please keep the signature of the is_leader() method unchanged (though
implement the body of that function correctly.  You will need to add
other methods to implement ONLY the leader election part of the RAFT
protocol.
'''
class RaftNode(rpyc.Service):
        """
        Initialize the class using the config file provided and also initialize
        any datastructures you may need.
        """
        def __init__(self, config, node):
                number_of_nodes = 1
                nodes, hosts, ports = list(), list(), list()
                
                # Read in the config file
                with open(config) as file:
                        # Split based on newlines
                        config_lines = file.read().splitlines()
                        for i, line in enumerate(config_lines):
                                # Parse the lines of the config file by removing whitespace and delimiting by colons
                                line = line.replace(' ','').split(':')
                                
                                # The first line of the config file is reserved for the number of ports
                                if i == 0:
                                        number_of_nodes = int(line[1])
                                # All but the first line of the config file contain node, host, and port information
                                else:
                                        nodes.append(int(line[0][4:]))
                                        hosts.append(line[1])
                                        ports.append(int(line[2]))

                # Initialize the node with the parsed config data (these are final)
                self.node = node
                self.number_of_nodes = number_of_nodes
                self.majority = number_of_nodes // 2 + 1
                self.nodes = nodes
                self.hosts = hosts
                self.ports = ports

                # Intialize the node with locks
                self.append_entries_lock = Lock()
                self.request_vote_lock = Lock()
                self.election_lock = Lock()

                # Create directories and loggers for storage persistence
                directory = os.getcwd() + '/tmp'

                self.state_directory = directory + '/state'
                if not os.path.exists(self.state_directory):
                        os.makedirs(self.state_directory)

                self.debug_directory = directory + '/debug'
                if not os.path.exists(self.debug_directory):
                        os.makedirs(self.debug_directory)

                self.state_logfile = self.state_directory + '/' + str(self.node)
                self.state_logger = logging.getLogger('state')
                self.state_logger.setLevel(logging.INFO)
                state_handler = logging.FileHandler(self.state_logfile)
                self.state_logger.addHandler(state_handler)
                
                self.debug_logfile = self.debug_directory + '/' + str(self.node)
                self.debug_logger = logging.getLogger('debug')
                self.debug_logger.setLevel(logging.DEBUG)
                debug_handler = logging.FileHandler(self.debug_logfile)
                debug_handler.setLevel(logging.DEBUG)
                debug_format = logging.Formatter('%(asctime)s: %(message)s')
                debug_handler.setFormatter(debug_format)
                self.debug_logger.addHandler(debug_handler)
                
                # Initialize the node with state information, using storage persisted state if it exists
                if os.stat(self.state_logfile).st_size == 0:
                        self.term = 0
                        self.vote = -1
                else:
                        with open(self.state_logfile) as file:
                                restore_term, restore_vote = file.read().splitlines()[-1].split(',')
                                self.term = int(restore_term)
                                self.vote = int(restore_vote)
                self.state = 'follower'
                self.vote_count = 0
                self.leader = -1

                # Start the server
                self.server = Thread(target=self.run_server)
                self.server.start()
        '''
        x = is_leader(): returns True or False, depending on whether
        this node is a leader

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call

        CHANGE THIS METHOD TO RETURN THE APPROPRIATE RESPONSE
        '''
        def exposed_is_leader(self):
                return self.state == 'leader'

        def exposed_request_vote(self, candidate_term, candidate_ID):
                vote_granted = False
                debug_message = tuple()
                with self.request_vote_lock:
                        if self.vote == -1:
                                if self.term < candidate_term:
                                        self.timeout.cancel()
                                        self.term = candidate_term
                                        self.state = 'follower'
                                        self.leader = candidate_ID
                                        self.vote = candidate_ID
                                        self.vote_count = 0
                                        vote_granted = True
                                        debug_message = ('I am a follower of the candidate node {} with term {}'.format(candidate_ID, candidate_term), )
                                elif self.term == candidate_term:
                                        self.timeout.cancel()
                                        self.state = 'follower'
                                        self.leader = candidate_ID
                                        self.vote = candidate_ID
                                        self.vote_count = 0
                                        vote_granted = True
                                        debug_message = ('I am a follower of the candidate node {} with term {}'.format(candidate_ID, candidate_term), )
                                else: # self.term > candidate_term
                                        vote_granted = False
                                        debug_message = ('I have term {}, which is higher than that of the candidate node {} with term {}'.format(self.term, candidate_ID, candidate_term), )
                        else: # self.vote != -1
                                vote_granted = False
                                debug_message = ('I have already voted for candidate node {}'.format(self.vote), )
                        log_state_thread = Thread(target=self.log_state)
                        log_state_thread.start()
                        log_debug_thread = Thread(target=self.log_debug, args=debug_message)
                        log_debug_thread.start()
                return self.term, vote_granted 

        def exposed_append_entries(self, leader_term, leader_ID):
                success = False
                debug_message = tuple()
                with self.append_entries_lock:
                        if self.term < leader_term:
                                self.timeout.cancel()
                                self.state = 'follower'
                                self.leader = leader_ID
                                self.term = leader_term
                                success = True
                                debug_message = ('I am a follower of the leader node {} with term {}'.format(leader_ID, leader_term), )
                        elif self.term == leader_term:
                                self.timeout.cancel()
                                self.state = 'follower'
                                self.leader = leader_ID
                                success = True
                                debug_message = ('I am a follower of the leader node {} with term {}'.format(leader_ID, leader_term), )
                        else: #self.term > leader_term
                                success = False
                                debug_message = ('I have term {}, which is higher than that of the leader node {} with term {}'.format(self.term, leader_ID, leader_term), )
                        log_state_thread = Thread(target=self.log_state)
                        log_state_thread.start()
                        log_debug_thread = Thread(target=self.log_debug, args=debug_message)
                        log_debug_thread.start()
                return self.term, success

        def run_server(self):
                while True:
                        timeout = uniform(1, 6)
                        self.vote = -1
                        self.vote_count = 0
                        log_state_thread = Thread(target=self.log_state)
                        log_state_thread.start()
                        if self.state == 'follower':
                                self.timeout = Timer(timeout, self.follower_timeout)
                        elif self.state == 'candidate':
                                self.timeout = Timer(timeout, self.candidate_timeout)
                        elif self.state == 'leader':
                                self.timeout = Timer(0.1, self.broadcast_heartbeat)
                        else:
                                sys.exit()
                        self.timeout.start()
                        self.timeout.join()

        def follower_timeout(self):
                self.state = 'candidate'
                self.term = self.term + 1
                self.vote = self.node
                self.vote_count = 1
                self.leader = -1
                
                log_state_thread = Thread(target=self.log_state)
                log_state_thread.start()
                debug_message = ('I timed out as a follower', )
                log_debug_thread = Thread(target=self.log_debug, args=debug_message)
                log_debug_thread.start()
                self.broadcast_election()

        def candidate_timeout(self):
                self.term = self.term + 1
                self.vote = self.node
                self.vote_count = 1
                self.leader = -1
                
                log_state_thread = Thread(target=self.log_state)
                log_state_thread.start()
                debug_message = ('I timed out as a candidate', )
                log_debug_thread = Thread(target=self.log_debug, args=debug_message)
                log_debug_thread.start()
                self.broadcast_election()

        def election(self, candidate_term, candidate_ID, host, port):
                debug_message = tuple()
                try:
                        connection = rpyc.connect(host, port)
                        term, vote_granted = connection.root.request_vote(candidate_term, candidate_ID)
                        with self.election_lock:
                                if vote_granted:
                                        self.vote_count = self.vote_count + 1
                                        log_state_thread = Thread(target=self.log_state)
                                        log_state_thread.start()     
                                        debug_message = ('Vote granted from node at host {} and port {}'.format(host, port), )
                                else:
                                        debug_message = ('Vote not granted from node at host {} and port {}'.format(host, port), )
                except Exception:
                        debug_message = ('Unsuccessful RequestVote RPC to node at host {} and port {}'.format(host, port), )
                finally:
                        log_debug_thread = Thread(target=self.log_debug, args=debug_message)
                        log_debug_thread.start()

        def broadcast_election(self):
                debug_message = ('I am starting an election', )
                log_debug_thread = Thread(target=self.log_debug, args=debug_message)
                log_debug_thread.start()
                
                election_threads = list()
                for (node, host, port) in zip(self.nodes, self.hosts, self.ports):
                        if node != self.node:
                                election_arguments = (self.term, self.node, host, port)
                                election_thread = Thread(target=self.election, args=election_arguments)
                                election_threads.append(election_thread)
                                election_thread.start()
                for election_thread in election_threads:
                        election_thread.join()
                if self.vote_count >= self.majority:
                        debug_message = ('I am now the leader after winning the election with {} out of {} votes granted'.format(self.vote_count, self.number_of_nodes), )
                        
                        self.state = 'leader'
                        self.vote = -1
                        self.vote_count = 0
                        self.leader = self.node
                        
                        log_state_thread = Thread(target=self.log_state)
                        log_state_thread.start()
                        log_debug_thread = Thread(target=self.log_debug, args=debug_message)
                        log_debug_thread.start()
                        self.broadcast_heartbeat()
                else:
                        debug_message = ('I am still a candidate after not winning the election with {} out of {} votes granted'.format(self.vote_count, self.number_of_nodes), )
                        log_debug_thread = Thread(target=self.log_debug, args=debug_message)
                        log_debug_thread.start()

        def heartbeat(self, leader_term, leader_ID, host, port):
                debug_message = tuple()
                try:
                        connection = rpyc.connect(host, port)
                        term, success = connection.root.append_entries(leader_term, leader_ID)
                        with self.election_lock:
                                if not success:
                                        self.state = 'follower'
                                        self.term = term
                                        self.vote_count = 0
                                        self.leader = -1

                                        log_state_thread = Thread(target=self.log_state)
                                        log_state_thread.start()
                                        debug_message = ('I am now a follower after a unsuccessful heartbeat to node at host {} and port {}'.format(host, port), )
                                else:
                                        debug_message = ('I am still the leader after a successful heartbeat to node at host {} and port {}'.format(host, port), )
                except Exception:
                        debug_message = ('Unsuccessful AppendEntries RPC to node at host {} and port {}'.format(host, port), )
                finally:
                        log_debug_thread = Thread(target=self.log_debug, args=debug_message)
                        log_debug_thread.start()

        def broadcast_heartbeat(self):
                debug_message = ('I am sending a heartbeat with term {}'.format(self.term), )
                log_debug_thread = Thread(target=self.log_debug, args=debug_message)
                log_debug_thread.start()
                
                heartbeat_threads = list()
                for (node, host, port) in zip(self.nodes, self.hosts, self.ports):
                        if node != self.node:
                                heartbeat_arguments = (self.term, self.node, host, port)
                                heartbeat_thread = Thread(target=self.heartbeat, args=heartbeat_arguments)
                                heartbeat_threads.append(heartbeat_thread)
                                heartbeat_thread.start()
                for heartbeat_thread in heartbeat_threads:
                        heartbeat_thread.join()

        def log_state(self):
                state_information = str(self.term) + ',' + str(self.vote)
                self.state_logger.info(state_information)

        def log_debug(self, message):
                self.debug_logger.debug(message)

if __name__ == '__main__':
        number_of_arguments = len(sys.argv)
        if number_of_arguments != 4:
                raise TypeError('{} arguments given. \n Please supply 3 arguments: config, host, and port. '.format(number_of_arguments - 1))
                sys.exit(1)
        server = ThreadPoolServer(RaftNode(sys.argv[1], int(sys.argv[2])), port = int(sys.argv[3]))
        server.start()
