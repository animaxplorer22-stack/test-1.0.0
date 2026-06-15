#!/usr/bin/env python3
"""
DEVICE 2: Pure Miner (No Node)
Connects to Device 1's node via WebSocket
Run this AFTER Device 1 is running.
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

# ==================== CONFIGURATION ====================
# CHANGE THIS TO DEVICE 1's IP ADDRESS
DEVICE_1_IP = "192.168.1.100"  # ← PUT YOUR DEVICE 1 IP HERE
DEVICE_1_WS_PORT = 8080

class Device2Miner:
    def __init__(self, node_ip):
        self.node_ip = node_ip
        self.node_url = f"ws://{node_ip}:{DEVICE_1_WS_PORT}"
        
        # Generate wallet
        self.wallet, self.priv, self.pub = generate_test_wallet()
        self.username = f"miner_{self.wallet[:8]}"
        self.validator_id = simple_hash(f"{self.username}{self.pub}")[:32]
        
        self.ws = None
        self.connected = False
        self.is_validator = False
        self.current_challenge = ""
        self.current_block_id = 0
        self.last_challenge_time = 0
        
        # Stats
        self.rewards = 0
        self.blocks_signed = 0
        
        print("\n" + "=" * 60)
        print("DEVICE 2 - PURE MINER (No Node)")
        print("=" * 60)
        print(f"Username: {self.username}")
        print(f"Wallet: {self.wallet}")
        print(f"Validator ID: {self.validator_id[:16]}...")
        print(f"Connecting to: {self.node_url}")
        print("=" * 60)
        print("\n⚠️  Make sure Device 1 is running first!\n")
    
    async def register(self):
        """Register with the node"""
        timestamp = time.time()
        reg_message = f"{self.validator_id}{self.username}{100}{timestamp}"
        signature = simple_hash(f"{self.priv}{reg_message}")
        
        msg = {
            "type": "register",
            "validator_id": self.validator_id,
            "username": self.username,
            "public_key": self.pub,
            "wallet": self.wallet,
            "stake": 100,
            "level": 1,
            "rewards": 0,
            "blocks": 0,
            "uptime": 0,
            "miner_type": "test",
            "timestamp": timestamp,
            "signature": signature
        }
        
        if self.ws:
            await self.ws.send(json.dumps(msg))
            print(f"[REG] Sent registration for '{self.username}'")
    
    async def sign_block(self):
        """Sign the current challenge"""
        message = f"{self.current_challenge}{self.validator_id}{self.current_block_id}"
        signature = simple_hash(f"{self.priv}{message}")
        
        msg = {
            "type": "block_signature",
            "validator_id": self.validator_id,
            "username": self.username,
            "challenge": self.current_challenge,
            "signature": signature,
            "level": 1,
            "stake": 100,
            "block_id": self.current_block_id,
            "timestamp": time.time()
        }
        
        if self.ws:
            await self.ws.send(json.dumps(msg))
            print(f"[SIGN] ✍️ Signed block {self.current_block_id}")
            self.blocks_signed += 1
    
    async def send_uptime(self):
        """Send uptime ping"""
        msg = {
            "type": "uptime_ping",
            "validator_id": self.validator_id,
            "username": self.username,
            "uptime_seconds": 30,
            "stake": 100,
            "level": 1
        }
        if self.ws:
            await self.ws.send(json.dumps(msg))
    
    async def handle_message(self, data):
        """Handle messages from node"""
        try:
            msg = json.loads(data)
            msg_type = msg.get("type")
            
            if msg_type == "registered":
                print(f"[NODE] ✅ Registration confirmed! Level: {msg.get('level')}")
                print(f"   Reward per block: {msg.get('current_reward')} TEST")
            
            elif msg_type == "challenge":
                self.current_challenge = msg.get("challenge", "")
                self.current_block_id = msg.get("block_id", 0)
                self.last_challenge_time = time.time()
                self.is_validator = True
                await self.sign_block()
                
                # Set timeout
                async def timeout_handler():
                    await asyncio.sleep(2.5)
                    if self.is_validator:
                        print(f"[TIMEOUT] Missed block {self.current_block_id}")
                        self.is_validator = False
                
                asyncio.create_task(timeout_handler())
            
            elif msg_type == "block_accepted":
                reward = msg.get("reward", 0)
                self.rewards += reward
                self.is_validator = False
                print(f"[NODE] ✅ Block {msg.get('block_id')} ACCEPTED! +{reward} TEST")
                print(f"   Total rewards: {self.rewards} TEST")
            
            elif msg_type == "block_rejected":
                self.is_validator = False
                print(f"[NODE] ❌ Block {msg.get('block_id')} REJECTED")
            
            elif msg_type == "peers":
                print(f"[GOSSIP] Received {len(msg.get('peers', []))} peers from node")
            
            elif msg_type == "slash":
                print(f"[NODE] ⚠️ Slash command received!")
                self.is_validator = False
        
        except Exception as e:
            print(f"[ERROR] Message handling: {e}")
    
    async def connect_and_run(self):
        """Connect to node and start mining"""
        try:
            import websockets
            
            print(f"[CONN] Connecting to {self.node_url}...")
            async with websockets.connect(self.node_url, ping_interval=20, ping_timeout=10) as ws:
                self.ws = ws
                self.connected = True
                print(f"[CONN] ✅ Connected to node at {self.node_url}")
                
                # Request peers (gossip)
                await ws.send(json.dumps({"type": "get_peers"}))
                
                # Register
                await self.register()
                
                # Main loop
                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        await self.handle_message(message)
                    except asyncio.TimeoutError:
                        pass
                    
                    # Send uptime every 30 seconds
                    if int(time.time()) % 30 == 0:
                        await self.send_uptime()
                    
                    await asyncio.sleep(0.1)
        
        except ImportError:
            print("[ERROR] websockets not installed. Run: pip install websockets")
        except Exception as e:
            print(f"[CONN] ❌ Failed to connect: {e}")
            print(f"   Make sure Device 1 is running at {self.node_url}")

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--node', type=str, default=DEVICE_1_IP,
                        help=f'Device 1 IP (default: {DEVICE_1_IP})')
    args = parser.parse_args()
    
    miner = Device2Miner(node_ip=args.node)
    await miner.connect_and_run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[STOP] Device 2 stopped")
