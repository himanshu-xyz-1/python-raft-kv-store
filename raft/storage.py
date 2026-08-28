import os
import json
import zlib
import struct
import threading
from typing import Dict, Any, List, Optional, Tuple

class RaftStorage:
    """
    Production-grade Raft Storage with:
    1. Atomic Metadata Updates (meta.tmp -> flush -> fsync -> os.replace)
    2. Binary-Framed Write-Ahead Log (WAL) with CRC32 Checksums & Tail Recovery
    """
    def __init__(self, node_id: int, storage_dir: str = "cluster_data"):
        self.node_id = node_id
        self.storage_dir = storage_dir
        os.makedirs(self.storage_dir, exist_ok=True)
        self.wal_path = os.path.join(self.storage_dir, f"node_{node_id}.wal")
        self.meta_path = os.path.join(self.storage_dir, f"node_{node_id}.meta")
        self.lock = threading.Lock()

        self.current_term: int = 0
        self.voted_for: Optional[int] = None
        self.log: List[Dict[str, Any]] = []

        self._recover()

    def _recover(self):
        with self.lock:
            # 1. Recover Metadata
            if os.path.exists(self.meta_path):
                try:
                    with open(self.meta_path, "r", encoding="utf-8") as f:
                        meta = json.load(f)
                        self.current_term = meta.get("current_term", 0)
                        self.voted_for = meta.get("voted_for", None)
                except Exception:
                    self.current_term = 0
                    self.voted_for = None

            # 2. Recover WAL with CRC32 Verification & Corrupt Tail Discard
            self.log = []
            if os.path.exists(self.wal_path):
                valid_bytes_offset = 0
                with open(self.wal_path, "rb") as f:
                    while True:
                        offset = f.tell()
                        header = f.read(8)
                        if len(header) < 8:
                            break
                        length, expected_crc = struct.unpack("!II", header)
                        payload = f.read(length)
                        if len(payload) < length:
                            break
                        actual_crc = zlib.crc32(payload) & 0xffffffff
                        if actual_crc != expected_crc:
                            break  # Corrupt tail encountered
                        try:
                            entry = json.loads(payload.decode('utf-8'))
                            self.log.append(entry)
                            valid_bytes_offset = f.tell()
                        except Exception:
                            break

                # Truncate any incomplete/corrupt records left after crash
                with open(self.wal_path, "a+b") as f:
                    f.truncate(valid_bytes_offset)

    def persist_metadata(self, term: int, voted_for: Optional[int]):
        """Atomic metadata update using write-flush-fsync-replace"""
        with self.lock:
            self.current_term = term
            self.voted_for = voted_for
            temp_path = self.meta_path + ".tmp"
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump({"current_term": term, "voted_for": voted_for}, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, self.meta_path)

    def append_entry(self, term: int, op: str, key: str, val: Any = None) -> int:
        with self.lock:
            new_index = len(self.log) + 1
            entry = {"term": term, "index": new_index, "op": op, "key": key, "val": val}
            self.log.append(entry)
            self._write_wal_record(entry)
            return new_index

    def append_entries_from_leader(self, prev_log_index: int, prev_log_term: int, entries: List[Dict[str, Any]]) -> bool:
        with self.lock:
            # Check log match at prev_log_index
            if prev_log_index > 0:
                if len(self.log) < prev_log_index:
                    return False
                if self.log[prev_log_index - 1]["term"] != prev_log_term:
                    # Divergence found: truncate uncommitted records
                    self.log = self.log[:prev_log_index - 1]
                    self._rewrite_all_wal()
                    return False

            insert_idx = prev_log_index
            for entry in entries:
                if insert_idx < len(self.log):
                    if self.log[insert_idx]["term"] != entry["term"]:
                        self.log = self.log[:insert_idx]
                        self.log.append(entry)
                else:
                    self.log.append(entry)
                insert_idx += 1

            self._rewrite_all_wal()
            return True

    def _write_wal_record(self, entry: Dict[str, Any]):
        payload = json.dumps(entry).encode('utf-8')
        crc = zlib.crc32(payload) & 0xffffffff
        header = struct.pack("!II", len(payload), crc)
        with open(self.wal_path, "ab") as f:
            f.write(header + payload)
            f.flush()
            os.fsync(f.fileno())

    def _rewrite_all_wal(self):
        temp_wal = self.wal_path + ".tmp"
        with open(temp_wal, "wb") as f:
            for entry in self.log:
                payload = json.dumps(entry).encode('utf-8')
                crc = zlib.crc32(payload) & 0xffffffff
                header = struct.pack("!II", len(payload), crc)
                f.write(header + payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_wal, self.wal_path)

    def get_last_log_info(self) -> Tuple[int, int]:
        with self.lock:
            if not self.log:
                return (0, 0)
            return (self.log[-1]["term"], self.log[-1]["index"])

    def get_entry(self, index: int) -> Optional[Dict[str, Any]]:
        with self.lock:
            if 1 <= index <= len(self.log):
                return self.log[index - 1]
            return None

    def get_entries_from(self, next_index: int) -> List[Dict[str, Any]]:
        with self.lock:
            if next_index > len(self.log):
                return []
            return list(self.log[next_index - 1:])

    def clear(self):
        with self.lock:
            self.log.clear()
            self.current_term = 0
            self.voted_for = None
            if os.path.exists(self.wal_path):
                os.remove(self.wal_path)
            if os.path.exists(self.meta_path):
                os.remove(self.meta_path)
