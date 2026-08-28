import threading
from typing import Dict, Any, Optional

class KVStateMachine:
    """Decoupled Key-Value State Machine applied strictly up to commit_index."""
    def __init__(self):
        self.lock = threading.Lock()
        self.kv_store: Dict[str, Any] = {}
        self.last_applied: int = 0

    def apply(self, index: int, op: str, key: str, val: Any = None) -> Any:
        with self.lock:
            if index <= self.last_applied:
                return self.kv_store.get(key, None)
            
            if op == "SET":
                self.kv_store[key] = val
            elif op == "DEL" and key in self.kv_store:
                del self.kv_store[key]
            
            self.last_applied = index
            return self.kv_store.get(key, None)

    def get(self, key: str) -> Optional[Any]:
        with self.lock:
            return self.kv_store.get(key, None)

    def get_all(self) -> Dict[str, Any]:
        with self.lock:
            return dict(self.kv_store)

    def clear(self):
        with self.lock:
            self.kv_store.clear()
            self.last_applied = 0
