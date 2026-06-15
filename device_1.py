#!/usr/bin/env python3
"""
TEST GOSSIP - DEVICE 1 (Node + Miner)
This device runs:
- A FULL NODE (port 8081 for P2P, 8080 for WebSocket)
- An EMBEDDED MINER
- Acts as BOOTSTRAP node for Device 2

Run on Device 1 (your PC, Raspberry Pi, or main server)
"""

import asyncio
import json
import time
import hashlib
import random
import secrets
import argparse
import threading

# ==================== SIMPLE CRYPTO ====================
def simple_hash(data):
    return hashlib.sha256(data.encode()).hexdigest()

def generate_test_wallet():
    priv = secrets.token_hex(32)
    pub = simple_hash(priv)
    addr = "TEST_" + simple_hash(pub)[:32].upper()
    return addr, priv, pub

# ==================== CONFIGURATION ====================
NODE_HOST = "0.0.0.0"
NODE_PORT = 8080
P2P_PORT = 8081
BOOTSTRAP_PEERS = []  # This node is the bootstrap, so empty initially

# ==================== FULL GOSSIP NODE WITH MINER ====================
class FullGossipNode:
    def __init__(self, is_genesis=True, bootstrap_peer=None):
        self.is_genesis = is_genesis
        self.bootstrap_peer = bootstrap_peer
        self.peers = set()  # Discovered peers (IP:PORT)
        self.p2p_connections = {}  # Active P2P connections
        self.height = 0
        self.last_hash = "0" * 64
        self.pending_challenges = {}
        self.miners = {}  # Registered miners
        self.balance = 0
        self.pending_txs = []
        
        # Generate test wallet
        self.wallet, self.priv, self.pub = generate_test_wallet()
        self.username = f"device1_{self.wallet[:8]}"
        
        print(f"\n{'='*60}")
        print(f"DEVICE 1 - GOSSIP NODE + MINER")
        print(f"{'='*60}")
        print(f"Username: {self.username}")
        print(f"Wallet: {self.wallet}")
        print(f"Private Key: {self.priv[:16]}...")
        
        if is_genesis:
            self.balance = 100000
            print(f"Genesis balance: 100,000 TEST")
            print(f"Role: BOOTSTRAP NODE (other nodes will connect to this IP)")
        else:
            self.balance = 0
            print(f"Role: FOLLOWER NODE")
        
        # Get local IP for display
        self.local_ip = self._get_local_ip()
        print(f"Your Device 1 IP: {self.local_ip}")
        print(f"P2P Port: {P2P_PORT}")
        print(f"WebSocket Port: {NODE_PORT}")
        print(f"{'='*60}\n")
    
    def _get_local_ip(self):
        """Get local IP address"""
        try:
            import socket
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except:
            return "127.0.0.1"
    
    # ==================== P2P SERVER ====================
    async def start_p2p_server(self):
        """Start P2P server to accept peer connections"""
        self.server = await asyncio.start_server(self._handle_p2p, NODE_HOST, P2P_PORT)
        print(f"[P2P] Server listening on {self.local_ip}:{P2P_PORT}")
    
    async def _handle_p2p(self, reader, writer):
        """Handle incoming P2P connections"""
        addr = writer.get_extra_info('peername')
        addr_str = f"{addr[0]}:{addr[1]}"
        
        try:
            # Read message length
            length_data = await reader.read(4)
            if not length_data:
                writer.close()
                return
            
            msg_len = int.from_bytes(length_data, 'big')
            data = await reader.read(msg_len)
            msg = json.loads(data.decode())
            
            msg_type = msg.get("type")
            
            if msg_type == "handshake":
                # New peer connecting
                self.peers.add(addr_str)
                self.p2p_connections[addr_str] = writer
                peer_height = msg.get("height", 0)
                peer_username = msg.get("username", "unknown")
                print(f"[P2P] 🤝 New peer connected: {addr_str} ({peer_username}) height={peer_height}")
                
                # Send our peer list back (GOSSIP!)
                response = {
                    "type": "peer_list",
                    "peers": list(self.peers),
                    "height": self.height,
                    "username": self.username
                }
                response_data = json.dumps(response).encode()
                writer.write(len(response_data).to_bytes(4, 'big') + response_data)
                await writer.drain()
                
                # If peer is ahead, request blocks
                if peer_height > self.height:
                    await self._request_blocks(addr_str, self.height, peer_height)
            
            elif msg_type == "get_peers":
                # Peer asking for our peer list (GOSSIP!)
                response = {
                    "type": "peer_list",
                    "peers": list(self.peers),
                    "height": self.height,
                    "username": self.username
                }
                response_data = json.dumps(response).encode()
                writer.write(len(response_data).to_bytes(4, 'big') + response_data)
                await writer.drain()
                print(f"[P2P] 📡 Sent peer list to {addr_str} ({len(self.peers)} peers)")
            
            elif msg_type == "peer_list":
                # Received peer list from peer (GOSSIP! - DISCOVER NEW PEERS)
                new_peers = msg.get("peers", [])
                for p in new_peers:
                    if p != addr_str and p != f"{self.local_ip}:{P2P_PORT}" and p not in self.peers:
                        self.peers.add(p)
                        print(f"[GOSSIP] 🎉 Discovered new peer: {p}")
                        # Connect to the new peer
                        asyncio.create_task(self._connect_to_peer(p))
            
            elif msg_type == "get_blocks":
                start = msg.get("start", 0)
                end = msg.get("end", self.height)
                print(f"[P2P] 📦 Block request from {addr_str}: {start} to {end}")
                # Send mock blocks (for testing)
                blocks = []
                for bid in range(start, min(end, self.height)):
                    blocks.append({
                        "id": bid,
                        "hash": simple_hash(f"block_{bid}"),
                        "timestamp": time.time()
                    })
                response = {
                    "type": "blocks",
                    "blocks": blocks,
                    "count": len(blocks)
                }
                response_data = json.dumps(response).encode()
                writer.write(len(response_data).to_bytes(4, 'big') + response_data)
                await writer.drain()
            
            elif msg_type == "blocks":
                blocks = msg.get("blocks", [])
                print(f"[SYNC] 📥 Received {len(blocks)} blocks from {addr_str}")
                for block in blocks:
                    if block["id"] >= self.height:
                        self.height = block["id"] + 1
                        self.last_hash = block["hash"]
                        print(f"[SYNC] Block {block['id']} synced, new height: {self.height}")
            
            elif msg_type == "new_block":
                block = msg.get("block", {})
                print(f"[P2P] 🆕 New block #{block.get('id')} from {addr_str}")
                if block.get("id") == self.height:
                    self.height += 1
                    self.last_hash = block.get("hash", self.last_hash)
                    print(f"[P2P] Block accepted, new height: {self.height}")
            
            elif msg_type == "ping":
                response = {"type": "pong", "timestamp": time.time()}
                response_data = json.dumps(response).encode()
                writer.write(len(response_data).to_bytes(4, 'big') + response_data)
                await writer.drain()
            
            elif msg_type == "pong":
                # Keep connection alive
                pass
            
            writer.close()
            
        except Exception as e:
            print(f"[P2P] Error: {e}")
            writer.close()
    
    # ==================== CONNECT TO PEERS ====================
    async def _connect_to_peer(self, peer_addr):
        """Connect to a peer (initiate connection)"""
        if peer_addr in self.p2p_connections:
            return
        
        try:
            host, port = peer_addr.split(":")
            reader, writer = await asyncio.open_connection(host, int(port))
            
            # Send handshake
            handshake = {
                "type": "handshake",
                "height": self.height,
                "username": self.username,
                "node_id": self.wallet[:16]
            }
            data = json.dumps(handshake).encode()
            writer.write(len(data).to_bytes(4, 'big') + data)
            await writer.drain()
            
            # Wait for peer list response
            length_data = await reader.read(4)
            if length_data:
                msg_len = int.from_bytes(length_data, 'big')
                response_data = await reader.read(msg_len)
                response = json.loads(response_data.decode())
                
                if response.get("type") == "peer_list":
                    # Discover peers from the response (GOSSIP!)
                    for p in response.get("peers", []):
                        if p not in self.peers and p != f"{self.local_ip}:{P2P_PORT}":
                            self.peers.add(p)
                            print(f"[GOSSIP] 🎉 Discovered peer from handshake: {p}")
                            # Recursively connect to new peers
                            asyncio.create_task(self._connect_to_peer(p))
            
            writer.close()
            self.peers.add(peer_addr)
            print(f"[P2P] ✅ Successfully connected to: {peer_addr}")
            
        except Exception as e:
            print(f"[P2P] ❌ Failed to connect to {peer_addr}: {e}")
    
    # ==================== GOSSIP DISCOVERY LOOP ====================
    async def gossip_discovery_loop(self):
        """Periodically discover peers through gossip"""
        while True:
            await asyncio.sleep(15)  # Run every 15 seconds
            
            # First, connect to bootstrap peer if provided
            if self.bootstrap_peer and self.bootstrap_peer not in self.peers:
                print(f"[GOSSIP] Attempting to connect to bootstrap peer: {self.bootstrap_peer}")
                await self._connect_to_peer(self.bootstrap_peer)
            
            # Then ask all peers for their peer lists (gossip propagation)
            for peer in list(self.peers):
                try:
                    host, port = peer.split(":")
                    reader, writer = await asyncio.open_connection(host, int(port))
                    
                    request = {"type": "get_peers"}
                    data = json.dumps(request).encode()
                    writer.write(len(data).to_bytes(4, 'big') + data)
                    await writer.drain()
                    
                    # Wait for response
                    length_data = await reader.read(4)
                    if length_data:
                        msg_len = int.from_bytes(length_data, 'big')
                        response_data = await reader.read(msg_len)
                        response = json.loads(response_data.decode())
                        
                        if response.get("type") == "peer_list":
                            for p in response.get("peers", []):
                                if p not in self.peers and p != f"{self.local_ip}:{P2P_PORT}":
                                    self.peers.add(p)
                                    print(f"[GOSSIP] 🎉 Discovered new peer from {peer}: {p}")
                                    asyncio.create_task(self._connect_to_peer(p))
                    
                    writer.close()
                    
                except Exception as e:
                    print(f"[GOSSIP] Failed to query {peer}: {e}")
    
    # ==================== HEARTBEAT ====================
    async def heartbeat_loop(self):
        """Send periodic pings to keep connections alive"""
        while True:
            await asyncio.sleep(30)
            for peer in list(self.peers):
                try:
                    host, port = peer.split(":")
                    reader, writer = await asyncio.open_connection(host, int(port))
                    
                    ping = {"type": "ping", "timestamp": time.time()}
                    data = json.dumps(ping).encode()
                    writer.write(len(data).to_bytes(4, 'big') + data)
                    await writer.drain()
                    writer.close()
                    
                except Exception as e:
                    print(f"[HEARTBEAT] Peer {peer} unreachable: {e}")
                    if peer in self.peers:
                        self.peers.remove(peer)
    
    # ==================== MINER LOOP (Embedded) ====================
    async def embedded_miner_loop(self):
        """Simulate mining blocks (for testing)"""
        if not self.is_genesis:
            # Follower nodes don't mine in this test
            return
        
        while True:
            await asyncio.sleep(20)  # Mine every 20 seconds
            
            # Simulate mining a block
            block_id = self.height
            reward = 10
            
            # Create block
            block_hash = simple_hash(f"block_{block_id}_{time.time()}")
            block = {
                "id": block_id,
                "hash": block_hash,
                "timestamp": time.time(),
                "miner": self.username,
                "reward": reward
            }
            
            # Update state
            self.height += 1
            self.last_hash = block_hash
            self.balance += reward
            
            print(f"\n[⛏️ MINER] Block #{block_id} MINED!")
            print(f"   Hash: {block_hash[:16]}...")
            print(f"   Reward: +{reward} TEST")
            print(f"   New Balance: {self.balance} TEST")
            
            # Broadcast to peers
            await self._broadcast_new_block(block)
    
    async def _broadcast_new_block(self, block):
        """Broadcast new block to all peers"""
        for peer in list(self.peers):
            try:
                host, port = peer.split(":")
                reader, writer = await asyncio.open_connection(host, int(port))
                
                message = {
                    "type": "new_block",
                    "block": block,
                    "sender": self.username
                }
                data = json.dumps(message).encode()
                writer.write(len(data).to_bytes(4, 'big') + data)
                await writer.drain()
                writer.close()
                
            except Exception as e:
                print(f"[BROADCAST] Failed to send block to {peer}: {e}")
    
    # ==================== STATUS REPORTER ====================
    async def status_reporter(self):
        """Print status every 30 seconds"""
        while True:
            await asyncio.sleep(30)
            print(f"\n{'='*50}")
            print(f"📊 DEVICE 1 STATUS")
            print(f"{'='*50}")
            print(f"Username: {self.username}")
            print(f"Balance: {self.balance} TEST")
            print(f"Block Height: {self.height}")
            print(f"Connected Peers: {len(self.peers)}")
            if self.peers:
                print(f"Peer List: {list(self.peers)[:5]}")
            print(f"Miners Registered: {len(self.miners)}")
            print(f"Role: {'GENESIS (Mining)' if self.is_genesis else 'FOLLOWER'}")
            print(f"{'='*50}\n")
    
    # ==================== WEBSOCKET SERVER (For miners) ====================
    async def websocket_server(self):
        """Simple WebSocket server for external miners to connect"""
        try:
            import websockets
            from websockets.server import serve
            
            async def ws_handler(websocket, path):
                async for message in websocket:
                    try:
                        data = json.loads(message)
                        if data.get("type") == "register":
                            vid = data.get("validator_id")
                            username = data.get("username")
                            self.miners[vid] = {
                                "username": username,
                                "connected_at": time.time()
                            }
                            await websocket.send(json.dumps({
                                "type": "registered",
                                "level": 1,
                                "current_reward": 10,
                                "message": "Miner registered successfully"
                            }))
                            print(f"[MINER] ✅ Registered miner: {username} (ID: {vid[:16]}...)")
                        elif data.get("type") == "block_signature":
                            print(f"[MINER] ✍️ Received block signature from {data.get('username')}")
                    except Exception as e:
                        print(f"[WS] Error: {e}")
            
            async with serve(ws_handler, NODE_HOST, NODE_PORT):
                print(f"[WS] 🔌 WebSocket server on port {NODE_PORT} (for miners)")
                await asyncio.Future()
        except ImportError:
            print(f"[WS] ⚠️ WebSocket disabled (install: pip install websockets)")
            await asyncio.Future()
    
    # ==================== RUN ====================
    async def run(self):
        print("\n[START] Device 1 Gossip Node + Miner starting...\n")
        
        # Start all services
        await self.start_p2p_server()
        
        # Start background tasks
        asyncio.create_task(self.gossip_discovery_loop())
        asyncio.create_task(self.heartbeat_loop())
        asyncio.create_task(self.status_reporter())
        asyncio.create_task(self.embedded_miner_loop())
        
        # Start WebSocket server (optional)
        await self.websocket_server()

# ==================== MAIN ====================
async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--genesis', action='store_true', default=True, 
                        help='Run as genesis node (default: True)')
    parser.add_argument('--peer', type=str, 
                        help='Bootstrap peer to connect to (IP:PORT)')
    args = parser.parse_args()
    
    node = FullGossipNode(is_genesis=args.genesis, bootstrap_peer=args.peer)
    
    # If peer provided, connect immediately
    if args.peer:
        print(f"[BOOTSTRAP] Connecting to initial peer: {args.peer}")
        await node._connect_to_peer(args.peer)
    
    await node.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Device 1 stopped")