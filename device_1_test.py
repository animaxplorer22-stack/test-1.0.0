#!/usr/bin/env python3
"""
DEVICE 1: Full Node + Embedded Miner
Run this FIRST. This is the blockchain node.
"""

import asyncio
import json
import time
import hashlib
import secrets
import argparse

def simple_hash(data):
    return hashlib.sha256(data.encode()).hexdigest()

def generate_test_wallet():
    priv = secrets.token_hex(32)
    pub = simple_hash(priv)
    addr = "TEST_" + simple_hash(pub)[:32].upper()
    return addr, priv, pub

NODE_HOST = "0.0.0.0"
NODE_PORT = 8080   # WebSocket port for miners
P2P_PORT = 8081    # P2P port for other nodes

class Device1FullNode:
    def __init__(self):
        self.wallet, self.priv, self.pub = generate_test_wallet()
        self.username = f"node1_{self.wallet[:8]}"
        self.peers = set()
        self.miners = {}  # Registered miners
        self.height = 0
        self.last_hash = "0" * 64
        self.balance = 100000  # Genesis gets 100k
        self.pending_challenges = {}
        
        self.local_ip = self._get_local_ip()
        
        print("\n" + "=" * 60)
        print("DEVICE 1 - FULL NODE + MINER")
        print("=" * 60)
        print(f"Username: {self.username}")
        print(f"Wallet: {self.wallet}")
        print(f"Genesis Balance: 100,000 TEST")
        print(f"Your IP for Device 2: {self.local_ip}")
        print(f"WebSocket Port (for miners): {NODE_PORT}")
        print("=" * 60)
        print("\n⚠️  Device 2 will need this IP to connect.\n")
    
    def _get_local_ip(self):
        try:
            import socket
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except:
            return "127.0.0.1"
    
    # ==================== WEBSOCKET SERVER (for miners) ====================
    async def start_websocket_server(self):
        try:
            import websockets
            from websockets.server import serve
            
            async def ws_handler(websocket, path):
                async for message in websocket:
                    try:
                        data = json.loads(message)
                        msg_type = data.get("type")
                        
                        if msg_type == "register":
                            # Miner registration
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
                            print(f"[MINER] ✅ Registered: {username} (ID: {vid[:16]}...)")
                        
                        elif msg_type == "block_signature":
                            # Miner signing a block
                            challenge = data.get("challenge")
                            vid = data.get("validator_id")
                            print(f"[MINER] ✍️ Received signature from {data.get('username')} for challenge {challenge[:16]}...")
                            
                            # Store signature
                            if challenge in self.pending_challenges:
                                self.pending_challenges[challenge]["sigs"][vid] = data.get("signature")
                        
                        elif msg_type == "uptime_ping":
                            # Miner uptime update
                            pass
                        
                        elif msg_type == "get_peers":
                            # Gossip: send known peers
                            await websocket.send(json.dumps({
                                "type": "peers",
                                "peers": list(self.peers)
                            }))
                    
                    except Exception as e:
                        print(f"[WS] Error: {e}")
            
            async with serve(ws_handler, NODE_HOST, NODE_PORT):
                print(f"[WS] WebSocket server on port {NODE_PORT} (for miners)")
                await asyncio.Future()
                
        except ImportError:
            print(f"[WS] ⚠️ WebSocket not available. Install: pip install websockets")
            await asyncio.Future()
    
    # ==================== P2P SERVER (for other nodes) ====================
    async def start_p2p_server(self):
        self.server = await asyncio.start_server(self._handle_p2p, NODE_HOST, P2P_PORT)
        print(f"[P2P] P2P server on port {P2P_PORT}")
    
    async def _handle_p2p(self, reader, writer):
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
                print(f"[P2P] ✅ Peer connected: {addr_str}")
                
                response = {
                    "type": "peer_list",
                    "peers": list(self.peers),
                    "height": self.height,
                    "username": self.username
                }
                response_data = json.dumps(response).encode()
                writer.write(len(response_data).to_bytes(4, 'big') + response_data)
                await writer.drain()
            
            writer.close()
            
        except Exception as e:
            print(f"[P2P] Error: {e}")
            writer.close()
    
    # ==================== BLOCK PRODUCTION ====================
    async def produce_block(self):
        """Produce a block (needs 1 validator for test)"""
        # For test, just use embedded miner as validator
        if len(self.miners) < 1:
            # No miners registered yet, wait
            return False
        
        block_id = self.height
        challenge = simple_hash(f"{block_id}{self.last_hash}{time.time()}{secrets.token_hex(8)}")
        
        # Select validators (for test, just use first registered miner)
        validators = list(self.miners.keys())[:1]
        
        self.pending_challenges[challenge] = {
            "bid": block_id,
            "validators": validators,
            "sigs": {},
            "start_time": time.time()
        }
        
        # Wait for signatures (2.5 seconds)
        await asyncio.sleep(2.5)
        
        pending = self.pending_challenges.pop(challenge, {})
        sigs = pending.get("sigs", {})
        
        if len(sigs) >= 1:
            # Block accepted
            reward = 10  # TEST reward
            self.height += 1
            self.last_hash = simple_hash(f"{block_id}{challenge}")
            self.balance += reward
            
            # Also give reward to the miner who signed
            for vid in sigs:
                if vid in self.miners:
                    # In real system, reward goes to miner's wallet
                    print(f"[REWARD] +{reward} TEST to miner {self.miners[vid]['username']}")
            
            print(f"\n[BLOCK #{block_id}] ✅ ACCEPTED | Validators: {len(sigs)} | Reward: +{reward} TEST")
            print(f"   Node balance: {self.balance} TEST")
            return True
        else:
            print(f"\n[BLOCK #{block_id}] ❌ REJECTED | Got {len(sigs)}/1 signatures")
            return False
    
    async def block_production_loop(self):
        """Produce blocks every 30 seconds"""
        while True:
            await self.produce_block()
            await asyncio.sleep(30)
    
    # ==================== EMBEDDED MINER (inside node) ====================
    async def embedded_miner_loop(self):
        """Node's own embedded miner"""
        while True:
            # Check for challenges to sign
            for challenge, pending in self.pending_challenges.items():
                if self.username in pending.get("validators", []) and self.username not in pending.get("sigs", {}):
                    # Sign the challenge
                    signature = simple_hash(f"{self.priv}{challenge}{self.username}{pending['bid']}")
                    pending["sigs"][self.username] = signature
                    print(f"[EMBEDDED MINER] ✍️ Signed block {pending['bid']}")
            await asyncio.sleep(0.2)
    
    # ==================== STATUS REPORTER ====================
    async def status_reporter(self):
        while True:
            await asyncio.sleep(30)
            print(f"\n📊 STATUS | Height: {self.height} | Balance: {self.balance} | Miners: {len(self.miners)} | Peers: {len(self.peers)}")
            if self.miners:
                print(f"   Registered miners: {list(self.miners.keys())[:3]}")
    
    # ==================== RUN ====================
    async def run(self):
        await self.start_p2p_server()
        asyncio.create_task(self.start_websocket_server())
        asyncio.create_task(self.block_production_loop())
        asyncio.create_task(self.embedded_miner_loop())
        asyncio.create_task(self.status_reporter())
        
        print("\n✅ DEVICE 1 RUNNING (Node + Miner)")
        print(f"   Device 2 should connect to: ws://{self.local_ip}:{NODE_PORT}")
        print("   Press Ctrl+C to stop.\n")
        await asyncio.Future()

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--genesis', action='store_true', default=True)
    args = parser.parse_args()
    
    node = Device1FullNode()
    await node.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[STOP] Device 1 stopped")
