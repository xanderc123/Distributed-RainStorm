import hashlib
import bisect

class ConsistentHashRing:
    def __init__(self, hash_space=2**32):
        self.hash_space = hash_space
        self.ring = []
        self.node_map = {}

    def _hash(self, key: str) -> int:
        # Use SHA-1, take lower 32 bits for ring position
        h = hashlib.sha1(key.encode()).hexdigest()
        return int(h, 16) % self.hash_space

    def add_node(self, node_id: str):
        token = self._hash(node_id)
        if token in self.node_map:
            raise ValueError(f"Duplicate token detected for {node_id}")
        bisect.insort(self.ring, token)
        self.node_map[token] = node_id

    def remove_node(self, node_id: str):
        token = self._hash(node_id)
        if token in self.node_map:
            self.ring.remove(token)
            del self.node_map[token]

    def get_node(self, key: str) -> str:
        """Return the node responsible for a given key"""
        key_hash = self._hash(key)
        idx = bisect.bisect(self.ring, key_hash) % len(self.ring)
        token = self.ring[idx]
        return self.node_map[token]

    def get_ring_id(self, node):
        """Returns RingID given IP:PORT"""
        for k, v in self.node_map.items():
            if node == v:
                return k
        return None 
