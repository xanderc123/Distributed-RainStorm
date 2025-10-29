import hashlib
import bisect
import random

HASH_SPACE = 2**32

class ConsistentHash:
    def __init__(self):
        self.ring = []                # sorted list of tokens
        self.token_to_node = {}       # token → node_id
        self.node_to_tokens = {}      # node_id → [tokens]

    def _hash(self, key: str) -> int:
        h = hashlib.sha1(key.encode()).hexdigest()
        return int(h, 16) % HASH_SPACE

    def add_node(self, node_id: str, n_tokens: int = 64):
        """Assign multiple tokens (vnodes) per node."""
        tokens = []
        for i in range(n_tokens):
            token = self._hash(f"{node_id}-{i}-{random.random()}")
            while token in self.token_to_node:
                token = self._hash(f"{node_id}-{i}-{random.random()}")
            bisect.insort(self.ring, token)
            self.token_to_node[token] = node_id
            tokens.append(token)
        self.node_to_tokens[node_id] = tokens

    def remove_node(self, node_id: str):
        tokens = self.node_to_tokens.pop(node_id, [])
        for token in tokens:
            self.ring.remove(token)
            del self.token_to_node[token]

    def get_successors(self, key: str, R: int = 3):
        """Return the R successor nodes responsible for a key."""
        if not self.ring:
            return []
        key_hash = self._hash(key)
        idx = bisect.bisect(self.ring, key_hash) % len(self.ring)
        successors = []
        for i in range(R):
            token = self.ring[(idx + i) % len(self.ring)]
            node = self.token_to_node[token]
            if node not in successors:
                successors.append(node)
        return successors
