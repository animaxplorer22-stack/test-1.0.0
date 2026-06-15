#!/usr/bin/env python3
"""
TEST GOSSIP - DEVICE 2 (Node + Miner)
This device runs:
- A FULL NODE (port 8081 for P2P, 8080 for WebSocket)
- An EMBEDDED MINER
- Connects to Device 1 via GOSSIP discovery

Run on Device 2 (laptop, phone, or another Raspberry Pi)
"""

import asyncio
import json
import time
import hashlib
import random
import secrets
import argparse

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

# CHANGE THIS TO DEVICE 1's IP ADDRESS
DEVICE_1_IP = "192.168.1.100"  # ← CHANGE THIS TO YOUR DEVICE 1's IP
BOOTSTRAP_PEER = f"{DEVICE_1_IP}:{P2P_PORT}"

# ==================== FULL GOSSIP NODE WITH MINER ====================
class FullGossipNodeFollower:
    def __init__(self, bootstrap_peer):
        self.bootstrap_peer = bootstrap_peer
        self.peers = set()  # Discovered peers (IP:PORT)
        self.p2p_connections = {}
        self.height = 0
        self.last_hash = "0" * 64
        self.pending_challenges = {}
        self.miners = {}
        self.balance = 0
        self.pending_txs = []
        
        # Generate test wallet
        self.wallet, self.priv, self.pub = generate_test_wallet()
        self.username = f"device2_{self.wallet[:8]}"
        
        print(f"\n{'='*60}")
        print(f"DEVICE 2 - GOSSIP NODE + MINER")
        print(f"{'='*60}")
        print(f"Username: {self.username}")
        print(f"Wallet: {self.wallet}")
        print(f"Bootstrap Peer: {bootstrap_peer}")
        print(f"Role: FOLLOWER NODE (will discover Device 1 via gossip)")
        
        # Get local IP for display
        self.local_ip = self._get_local_ip()
        print(f"Your Device 2 IP: {self.local_ip}")
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
            length_data = await reader.read(4)
            if not length_data:
                writer.close()
                return
            
            msg_len = int.from_bytes(length_data, 'big')
            data = await reader.read(msg_len)
            msg = json.loads(data.decode())
            
            msg_type = msg.get("type")
            
            if msg_type == "handshake":
                self.peers.add(addr_str)
                self.p2p_connections[addr_str] = writer
                print(f"[P2P] 🤝 New peer connected: {addr_str}")
                
                response = {
                    "type": "peer_list",
                    "peers": list(self.peers),
                    "height": self.height,
                    "username": self.username
                }
                response_data = json.dumps(response).encode()
                writer.write(len(response_data).to_bytes(4, 'big') + response_data)
                await writer.drain()
            
            elif msg_type == "get_peers":
                response = {
                    "type": "peer_list",
                    "peers": list(self.peers),
                    "height": self.height,
                    "username": self.username
                }
                response_data = json.dumps(response).encode()
                writer.write(len(response_data).to_bytes(4, 'big') + response_data)
                await writer.drain()
                print(f"[P2P] 📡 Sent peer list to {addr_str}")
            
            elif msg_type == "peer_list":
                new_peers = msg.get("peers", [])
                for p in new_peers:
                    if p != addr_str and p != f"{self.local_ip}:{P2P_PORT}" and p not in self.peers:
                        self.peers.add(p)
                        print(f"[GOSSIP] 🎉 Discovered new peer: {p}")
                        asyncio.create_task(self._connect_to_peer(p))
            
            elif msg_type == "new_block":
                block = msg.get("block", {})
                print(f"[P2P] 🆕 New block #{block.get('id')} from {addr_str}")
                if block.get("id") == self.height:
                    self.height += 1
                    self.last_hash = block.get("hash", self.last_hash)
                    # Add reward to balance if we're the miner (simplified)
                    if block.get("miner") == self.username:
                        reward = block.get("reward", 10)
                        self.balance += reward
                        print(f"[REWARD] +{reward} TEST to {self.username}")
            
            elif msg_type == "ping":
                response = {"type": "pong", "timestamp": time.time()}
                response_data = json.dumps(response).encode()
                writer.write(len(response_data).to_bytes(4, 'big') + response_data)
                await writer.drain()
            
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
            
            handshake = {
                "type": "handshake",
                "height": self.height,
                "username": self.username,
                "node_id": self.wallet[:16]
            }
            data = json.dumps(handshake).encode()
            writer.write(len(data).to_bytes(4, 'big') + data)
            await writer.drain()
            
            length_data = await reader.read(4)
            if length_data:
                msg_len = int.from_bytes(length_data, 'big')
                response_data = await reader.read(msg_len)
                response = json.loads(response_data.decode())
                
                if response.get("type") == "peer_list":
                    for p in response.get("peers", []):
                        if p not in self.peers and p != f"{self.local_ip}:{P2P_PORT}":
                            self.peers.add(p)
                            print(f"[GOSSIP] 🎉 Discovered peer from handshake: {p}")
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
            
            # Connect to bootstrap peer (Device 1)
            if self.bootstrap_peer and self.bootstrap_peer not in self.peers:
                print(f"[GOSSIP] 🔍 Attempting to connect to bootstrap peer: {self.bootstrap_peer}")
                await self._connect_to_peer(self.bootstrap_peer)
            
            # Ask all peers for their peer lists
            for peer in list(self.peers):
                try:
                    host, port = peer.split(":")
                    reader, writer = await asyncio.open_connection(host, int(port))
                    
                    request = {"type": "get_peers"}
                    data = json.dumps(request).encode()
                    writer.write(len(data).to_bytes(4, 'big') + data)
                    await writer.drain()
                    
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
    
    # ==================== STATUS REPORTER ====================
    async def status_reporter(self):
        """Print status every 30 seconds"""
        while True:
            await asyncio.sleep(30)
            print(f"\n{'='*50}")
            print(f"📊 DEVICE 2 STATUS")
            print(f"{'='*50}")
            print(f"Username: {self.username}")
            print(f"Balance: {self.balance} TEST")
            print(f"Block Height: {self.height}")
            print(f"Connected Peers: {len(self.peers)}")
            if self.peers:
                print(f"Peer List: {list(self.peers)[:5]}")
            print(f"Bootstrap Peer: {self.bootstrap_peer}")
            print(f"{'='*50}\n")
    
    # ==================== MINER LOOP (Embedded) ====================
    async def embedded_miner_loop(self):
        """Simulate mining blocks (simple for testing)"""
        while True:
            await asyncio.sleep(25)  # Mine every 25 seconds
            
            # Simulate mining a block
            block_id = self.height
            reward = 10
            
            block_hash = simple_hash(f"block_{block_id}_{time.time()}_{self.username}")
            block = {
                "id": block_id,
                "hash": block_hash,
                "timestamp": time.time(),
                "miner": self.username,
                "reward": reward
            }
            
            self.height += 1
            self.last_hash = block_hash
            self.balance += reward
            
            print(f"\n[⛏️ MINER] Block #{block_id} MINED by {self.username}!")
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
    
    # ==================== WEBSOCKET SERVER ====================
    async def websocket_server(self):
        """Simple WebSocket server for external miners"""
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
                            self.miners[vid] = {"username": username}
                            await websocket.send(json.dumps({
                                "type": "registered",
                                "level": 1,
                                "current_reward": 10
                            }))
                            print(f"[MINER] ✅ Registered miner: {username}")
                    except:
                        pass
            
            async with serve(ws_handler, NODE_HOST, NODE_PORT):
                print(f"[WS] 🔌 WebSocket server on port {NODE_PORT}")
                await asyncio.Future()
        except ImportError:
            print(f"[WS] ⚠️ WebSocket disabled (install: pip install websockets)")
            await asyncio.Future()
    
    # ==================== RUN ====================
    async def run(self):
        print("\n[START] Device 2 Gossip Node + Miner starting...")
        print(f"[CONFIG] Will connect to bootstrap: {self.bootstrap_peer}\n")
        
        await self.start_p2p_server()
        
        asyncio.create_task(self.gossip_discovery_loop())
        asyncio.create_task(self.heartbeat_loop())
        asyncio.create_task(self.status_reporter())
        asyncio.create_task(self.embedded_miner_loop())
        
        await self.websocket_server()

# ==================== MAIN ====================
async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--peer', type=str, default=BOOTSTRAP_PEER,
                        help=f'Bootstrap peer IP:PORT (default: {BOOTSTRAP_PEER})')
    args = parser.parse_args()
    
    node = FullGossipNodeFollower(bootstrap_peer=args.peer)
    
    # Immediately connect to bootstrap peer
    print(f"[BOOTSTRAP] Connecting to initial peer: {args.peer}")
    await node._connect_to_peer(args.peer)
    
    await node.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Device 2 stopped")