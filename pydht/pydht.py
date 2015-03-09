import json
import random
import socket
import threading
import time

try:
    # For Python 2.* : SocketServer
    import SocketServer
except ImportError:
    # For Python 3.* : SocketServer has been renamed to socketserver
    import socketserver as SocketServer

from .bucketset import BucketSet
from .hashing import hash_function, random_id
from .peer import Peer
from .shortlist import Shortlist

# Python3 support
import sys
if sys.version < '3':
    # Python 2.*
    def _unicode(x):
        return unicode(x)
    def _has_key(_dict, x):
        return _dict.has_key(x)
else:
    # Python 3.*
    def _unicode(x):
        return x
    def _has_key(_dict, x):
        return x in _dict

k = 20
alpha = 3
id_bits = 128
iteration_sleep = 1

class DHTRequestHandler(SocketServer.BaseRequestHandler):

    def handle(self):
        try:
            message_str = self.request[0].strip()
            if sys.version >= '3':
                message_str = message_str.decode('utf-8')
            message = json.loads(message_str)
            message_type = message["message_type"]
            if message_type == "ping":
                self.handle_ping(message)
            elif message_type == "pong":
                self.handle_pong(message)
            elif message_type == "find_node":
                self.handle_find(message)
            elif message_type == "find_value":
                self.handle_find(message, find_value=True)
            elif message_type == "found_nodes":
                self.handle_found_nodes(message)
            elif message_type == "found_value":
                self.handle_found_value(message)
            elif message_type == "store":
                self.handle_store(message)
        except (KeyError, ValueError) as e:
            pass
        client_host, client_port = self.client_address
        peer_id = message["peer_id"]
        new_peer = Peer(client_host, client_port, peer_id)
        self.server.dht.buckets.insert(new_peer)

    def handle_ping(self, message):
        client_host, client_port = self.client_address
        id = message["peer_id"]
        peer = Peer(client_host, client_port, id)
        peer.pong(socket=self.server.socket, peer_id=self.server.dht.peer.id, lock=self.server.send_lock)
        
    def handle_pong(self, message):
        client_host, client_port = self.client_address
        id = message["peer_id"]
        peer = self.server.dht.buckets.last_pong_received[(client_host, client_port, id)] = time.time()
        
    def handle_find(self, message, find_value=False):
        key = message["id"]
        id = message["peer_id"]
        client_host, client_port = self.client_address
        peer = Peer(client_host, client_port, id)
        response_socket = self.request[1]
        if find_value and (key in self.server.dht.data):
            value = self.server.dht.data[key]
            peer.found_value(id, value, message["rpc_id"], socket=response_socket, peer_id=self.server.dht.peer.id, lock=self.server.send_lock)
        else:
            nearest_nodes = self.server.dht.buckets.nearest_nodes(id)
            if not nearest_nodes:
                nearest_nodes.append(self.server.dht.peer)
            nearest_nodes = [nearest_peer.astriple() for nearest_peer in nearest_nodes]
            peer.found_nodes(id, nearest_nodes, message["rpc_id"], socket=response_socket, peer_id=self.server.dht.peer.id, lock=self.server.send_lock)

    def handle_found_nodes(self, message):
        rpc_id = message["rpc_id"]
        shortlist = self.server.dht.rpc_ids[rpc_id]
        del self.server.dht.rpc_ids[rpc_id]
        nearest_nodes = [Peer(*peer) for peer in message["nearest_nodes"]]
        shortlist.update(nearest_nodes)
        
    def handle_found_value(self, message):
        rpc_id = message["rpc_id"]
        shortlist = self.server.dht.rpc_ids[rpc_id]
        del self.server.dht.rpc_ids[rpc_id]
        shortlist.set_complete(message["value"])
        
    def handle_store(self, message):
        key = message["id"]
        self.server.dht.data[key] = message["value"]


class DHTServer(SocketServer.ThreadingMixIn, SocketServer.UDPServer):
    def __init__(self, host_address, handler_cls):
        SocketServer.UDPServer.__init__(self, host_address, handler_cls)
        self.send_lock = threading.Lock()

class DHT(object):
    def __init__(self, host, port, id=None, boot_host=None, boot_port=None):
        if not id:
            id = random_id()
        self.peer = Peer(_unicode(host), port, id)
        self.data = {}
        self.buckets = BucketSet(k, id_bits, self.peer.id)
        self.rpc_ids = {} # should probably have a lock for this
        self.server = DHTServer(self.peer.address(), DHTRequestHandler)
        self.server.dht = self
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.daemon = True
        self.server_thread.start()
        self.bootstrap(_unicode(boot_host), boot_port)
        self.is_dead = False
        self.checkalive_thread = threading.Thread(target=self.check_peers_alive)
        self.checkalive_thread.daemon = True
        self.checkalive_thread.start()
    
    def iterative_find_nodes(self, key, boot_peer=None):
        shortlist = Shortlist(k, key)
        shortlist.update(self.buckets.nearest_nodes(key, limit=alpha))
        if boot_peer:
            rpc_id = random.getrandbits(id_bits)
            self.rpc_ids[rpc_id] = shortlist
            boot_peer.find_node(key, rpc_id, socket=self.server.socket, peer_id=self.peer.id)
        while (not shortlist.complete()) or boot_peer:
            nearest_nodes = shortlist.get_next_iteration(alpha)
            for peer in nearest_nodes:
                shortlist.mark(peer)
                rpc_id = random.getrandbits(id_bits)
                self.rpc_ids[rpc_id] = shortlist
                peer.find_node(key, rpc_id, socket=self.server.socket, peer_id=self.peer.id) ######
            time.sleep(iteration_sleep)
            boot_peer = None
        return shortlist.results()
        
    def iterative_find_value(self, key):
        shortlist = Shortlist(k, key)
        shortlist.update(self.buckets.nearest_nodes(key, limit=alpha))
        while not shortlist.complete():
            nearest_nodes = shortlist.get_next_iteration(alpha)
            for peer in nearest_nodes:
                shortlist.mark(peer)
                rpc_id = random.getrandbits(id_bits)
                self.rpc_ids[rpc_id] = shortlist
                peer.find_value(key, rpc_id, socket=self.server.socket, peer_id=self.peer.id) #####
            time.sleep(iteration_sleep)
        return shortlist.completion_result()
            
    def bootstrap(self, boot_host, boot_port):
        if boot_host and boot_port:
            boot_peer = Peer(boot_host, boot_port, 0)
            self.iterative_find_nodes(self.peer.id, boot_peer=boot_peer)
                    
    def get_known_peers(self):
        return self.buckets.nearest_nodes(self.peer.id, limit=alpha)
    
    def check_peers_alive(self):
        while True:
            if self.is_dead:
                return
            known_peers = self.buckets.nearest_nodes(self.peer.id, limit=alpha)
            for p in known_peers:
                if _has_key(self.server.dht.buckets.last_pong_received,(p.host, p.port, p.id)):
                    # If this peer is dead
                    current_t = time.time()
                    last_peer_pong = self.server.dht.buckets.last_pong_received[(p.host, p.port, p.id)]
                    if current_t - last_peer_pong > 15: # If more than 10 seconds since last pong received => DEAD
                        self.server.dht.buckets.remove(p)
                        continue
                # Ping the still alive peers
                p.ping(socket=self.server.socket, peer_id=self.peer.id)
            
            time.sleep(10) # Check alive peers every 10 seconds
    
    def close(self):
        if self.is_dead:
            return
        # Shutdown socket server
        if sys.version < '3':
            self.server.shutdown()
        else:
            # For some reason, shutdowning() the server in Python 3 blocks in the thread
            # => the shutdown() method is called from the same thread as the serve_forever one,
            # which makes no sense..
            self.server.__shutdown_request = True
        # Stop pinging
        self.is_dead = True # Will stop loop for check alive thread
        
    def __getitem__(self, key):
        hashed_key = hash_function(key.encode('utf-8'))
        if hashed_key in self.data:
            return self.data[hashed_key]
        result = self.iterative_find_value(hashed_key)
        if result:
            return result
        raise KeyError
        
    def __setitem__(self, key, value):
        hashed_key = hash_function(key.encode('utf-8'))
        nearest_nodes = self.iterative_find_nodes(hashed_key)
        if not nearest_nodes:
            self.data[hashed_key] = value
        for node in nearest_nodes:
            node.store(hashed_key, value, socket=self.server.socket, peer_id=self.peer.id)
        
    def tick():
        pass
