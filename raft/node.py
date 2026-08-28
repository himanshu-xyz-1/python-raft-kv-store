import socket
import threading
import time
import random
import queue
from typing import Dict, Any, List, Optional, Tuple, Set
from .protocol import Message
from .storage import RaftStorage
from .state_machine import KVStateMachine

class RaftNode:
    def __init__(self, node_id: int, port: int, peers: Dict[int, int], event_bus: queue.Queue,
                 election_min_ms: int = 1500, election_max_ms: int = 3000, heartbeat_ms: int = 300):
        self.node_id = node_id
        self.port = port
        self.peers = peers  # {peer_id: peer_port}
        self.event_bus = event_bus

        # Persistence & State Machine
        self.storage = RaftStorage(node_id)
        self.state_machine = KVStateMachine()

        # Raft State (Persistent)
        self.current_term = self.storage.current_term
        self.voted_for = self.storage.voted_for

        # Raft State (Volatile)
        self.commit_index = 0
        self.last_applied = 0
        self.role = "FOLLOWER"  # FOLLOWER, CANDIDATE, LEADER, DEAD
        self.leader_id: Optional[int] = None
        self.votes_received: Set[int] = set()

        # Leader-only Volatile State
        self.next_index: Dict[int, int] = {}
        self.match_index: Dict[int, int] = {}

        # Timing Configs
        self.election_min = election_min_ms / 1000.0
        self.election_max = election_max_ms / 1000.0
        self.heartbeat_interval = heartbeat_ms / 1000.0
        self.election_timeout = random.uniform(self.election_min, self.election_max)
        self.last_heartbeat_time = time.time()

        # Network Partition Simulation Matrix (Set of blocked peer_ids)
        self.blocked_peers: Set[int] = set()

        # Metrics
        self.metrics = {
            "elections": 0, "election_wins": 0, "heartbeats_sent": 0,
            "heartbeats_rcvd": 0, "writes_committed": 0, "log_conflicts": 0
        }

        # Concurrency Control
        self.lock = threading.Lock()
        self.is_alive = True
        self.server_sock: Optional[socket.socket] = None

    def start(self):
        self.is_alive = True
        self.role = "FOLLOWER"
        self._recover_state_machine()
        threading.Thread(target=self._run_server, daemon=True).start()
        threading.Thread(target=self._run_ticker, daemon=True).start()
        self._emit_log(f"🟢 Initialized on port :{self.port} as FOLLOWER (Term {self.current_term})")

    def crash(self):
        with self.lock:
            self.is_alive = False
            self.role = "DEAD"
            self.leader_id = None
            if self.server_sock:
                try:
                    self.server_sock.close()
                except Exception:
                    pass
        self._emit_log("🔴 CRASHED: Process stopped.")

    def recover(self):
        with self.lock:
            if self.is_alive:
                return
            self.storage = RaftStorage(self.node_id)
            self.current_term = self.storage.current_term
            self.voted_for = self.storage.voted_for
            self.commit_index = 0
            self.last_applied = 0
            self.role = "FOLLOWER"
            self.is_alive = True
            self.last_heartbeat_time = time.time()
            self._recover_state_machine()

        threading.Thread(target=self._run_server, daemon=True).start()
        threading.Thread(target=self._run_ticker, daemon=True).start()
        last_t, last_i = self.storage.get_last_log_info()
        self._emit_log(f"♻️ RECOVERED: Re-joined cluster (Term {self.current_term}, Log #{last_i})")

    def _recover_state_machine(self):
        self.state_machine.clear()
        for entry in self.storage.log:
            self.state_machine.apply(entry["index"], entry["op"], entry["key"], entry["val"])
            self.last_applied = entry["index"]
            self.commit_index = entry["index"]

    def set_partition(self, peer_id: int, block: bool):
        with self.lock:
            if block:
                self.blocked_peers.add(peer_id)
            else:
                self.blocked_peers.discard(peer_id)

    def _emit_log(self, text: str):
        self.event_bus.put({
            "type": "NODE_LOG", "node_id": self.node_id,
            "text": text, "timestamp": time.strftime("%H:%M:%S")
        })

    def _run_server(self):
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.server_sock.bind(("127.0.0.1", self.port))
            self.server_sock.listen(20)
            while self.is_alive:
                try:
                    conn, _ = self.server_sock.accept()
                    threading.Thread(target=self._handle_client, args=(conn,), daemon=True).start()
                except Exception:
                    break
        except Exception:
            pass

    def _handle_client(self, conn: socket.socket):
        with conn:
            msg = Message.from_socket(conn)
            if not msg or not self.is_alive:
                return

            with self.lock:
                if msg.sender_id in self.blocked_peers:
                    return  # Drop packet in partition

            resp = self._process_message(msg)
            if resp:
                try:
                    conn.sendall(resp.serialize())
                except Exception:
                    pass

    def _process_message(self, msg: Message) -> Optional[Message]:
        with self.lock:
            if not self.is_alive:
                return None

            # 1. Update term if higher term discovered
            if msg.term > self.current_term:
                self.current_term = msg.term
                self.role = "FOLLOWER"
                self.voted_for = None
                self.storage.persist_metadata(self.current_term, None)
                self._emit_log(f"Step down to FOLLOWER (observed higher Term {msg.term} from Node {msg.sender_id})")

            # 2. REQUEST_VOTE
            if msg.msg_type == "REQUEST_VOTE":
                return self._handle_request_vote(msg)

            # 3. APPEND_ENTRIES
            elif msg.msg_type == "APPEND_ENTRIES":
                return self._handle_append_entries(msg)

            # 4. CLIENT REQUEST
            elif msg.msg_type == "CLIENT_REQ":
                if self.role != "LEADER":
                    return Message(self.node_id, "CLIENT_RESP", self.current_term, {
                        "success": False, "error": "NOT_LEADER", "leader_id": self.leader_id
                    })

        if msg.msg_type == "CLIENT_REQ":
            op = msg.payload.get("op", "SET")
            k = msg.payload.get("key", "")
            v = msg.payload.get("val", None)
            if op == "GET":
                val = self.state_machine.get(k)
                return Message(self.node_id, "CLIENT_RESP", self.current_term, {"success": True, "val": val})
            else:
                success = self.execute_client_write(k, v)
                return Message(self.node_id, "CLIENT_RESP", self.current_term, {"success": success})

        return None

    def _handle_request_vote(self, msg: Message) -> Message:
        cand_last_term = msg.payload.get("last_log_term", 0)
        cand_last_idx = msg.payload.get("last_log_idx", 0)
        my_last_term, my_last_idx = self.storage.get_last_log_info()

        # Correct Raft Election Safety Rule
        candidate_is_up_to_date = (
            cand_last_term > my_last_term or (
                cand_last_term == my_last_term and cand_last_idx >= my_last_idx
            )
        )
        can_vote = (self.voted_for is None or self.voted_for == msg.sender_id) and msg.term == self.current_term

        if can_vote and candidate_is_up_to_date:
            self.voted_for = msg.sender_id
            self.storage.persist_metadata(self.current_term, self.voted_for)
            self.last_heartbeat_time = time.time()
            self._emit_log(f"🗳️ Voted YES for Node {msg.sender_id} in Term {self.current_term}")
            return Message(self.node_id, "VOTE_ACK", self.current_term, {"vote_granted": True})
        else:
            reason = "already voted" if not can_vote else "candidate log not up-to-date"
            return Message(self.node_id, "VOTE_ACK", self.current_term, {"vote_granted": False, "reason": reason})

    def _handle_append_entries(self, msg: Message) -> Message:
        if msg.term < self.current_term:
            return Message(self.node_id, "APPEND_ACK", self.current_term, {"success": False, "match_index": 0})

        self.last_heartbeat_time = time.time()
        self.leader_id = msg.sender_id
        if self.role == "CANDIDATE":
            self.role = "FOLLOWER"

        prev_idx = msg.payload.get("prev_log_index", 0)
        prev_term = msg.payload.get("prev_log_term", 0)
        entries = msg.payload.get("entries", [])
        leader_commit = msg.payload.get("leader_commit", 0)

        success = self.storage.append_entries_from_leader(prev_idx, prev_term, entries)

        if success:
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.storage.log))
                self._apply_to_state_machine()

            _, current_last_idx = self.storage.get_last_log_info()
            return Message(self.node_id, "APPEND_ACK", self.current_term, {"success": True, "match_index": current_last_idx})
        else:
            self.metrics["log_conflicts"] += 1
            self._emit_log(f"⚠️ Log conflict rejected at prev_index={prev_idx} from Leader {msg.sender_id}")
            return Message(self.node_id, "APPEND_ACK", self.current_term, {"success": False, "match_index": len(self.storage.log)})

    def _apply_to_state_machine(self):
        while self.last_applied < self.commit_index:
            entry = self.storage.get_entry(self.last_applied + 1)
            if entry:
                self.state_machine.apply(entry["index"], entry["op"], entry["key"], entry["val"])
            self.last_applied += 1

    def _run_ticker(self):
        while self.is_alive:
            time.sleep(0.05)
            with self.lock:
                if not self.is_alive:
                    break

                if self.role in ("FOLLOWER", "CANDIDATE"):
                    if time.time() - self.last_heartbeat_time > self.election_timeout:
                        self._start_election()

    def _start_election(self):
        self.role = "CANDIDATE"
        self.current_term += 1
        self.voted_for = self.node_id
        self.storage.persist_metadata(self.current_term, self.node_id)
        self.votes_received = {self.node_id}
        self.last_heartbeat_time = time.time()
        self.election_timeout = random.uniform(self.election_min, self.election_max)
        self.metrics["elections"] += 1
        self._emit_log(f"⚡ Heartbeat timed out! Starting election for Term {self.current_term}")

        last_term, last_idx = self.storage.get_last_log_info()
        vote_msg = Message(self.node_id, "REQUEST_VOTE", self.current_term, {
            "last_log_term": last_term, "last_log_idx": last_idx
        })

        for peer_id, port in self.peers.items():
            threading.Thread(target=self._send_vote_req, args=(peer_id, port, vote_msg), daemon=True).start()

    def _send_vote_req(self, peer_id: int, port: int, msg: Message):
        resp = self._send_socket(port, msg)
        if resp and resp.payload.get("vote_granted"):
            with self.lock:
                if self.role == "CANDIDATE" and resp.term == self.current_term:
                    self.votes_received.add(peer_id)
                    total_nodes = len(self.peers) + 1
                    if len(self.votes_received) > (total_nodes // 2):
                        self.role = "LEADER"
                        self.leader_id = self.node_id
                        self.metrics["election_wins"] += 1
                        _, last_idx = self.storage.get_last_log_info()
                        for p in self.peers.keys():
                            self.next_index[p] = last_idx + 1
                            self.match_index[p] = 0
                        self._emit_log(f"👑 WON ELECTION with {len(self.votes_received)}/{total_nodes} votes. NOW LEADER (Term {self.current_term})!")
                        threading.Thread(target=self._leader_heartbeat_loop, daemon=True).start()

    def _leader_heartbeat_loop(self):
        while self.is_alive:
            with self.lock:
                if self.role != "LEADER":
                    break
            self._broadcast_append_entries()
            time.sleep(self.heartbeat_interval)

    def _broadcast_append_entries(self):
        with self.lock:
            if self.role != "LEADER":
                return
            last_t, last_i = self.storage.get_last_log_info()
            peers_snapshot = dict(self.peers)
            next_idx_snapshot = dict(self.next_index)

        for peer_id, port in peers_snapshot.items():
            with self.lock:
                if peer_id in self.blocked_peers:
                    continue
                nxt = next_idx_snapshot.get(peer_id, last_i + 1)
                prev_i = nxt - 1
                prev_e = self.storage.get_entry(prev_i)
                prev_t = prev_e["term"] if prev_e else 0
                entries = self.storage.get_entries_from(nxt)
                commit_idx = self.commit_index
                term = self.current_term

            msg = Message(self.node_id, "APPEND_ENTRIES", term, {
                "prev_log_index": prev_i, "prev_log_term": prev_t,
                "entries": entries, "leader_commit": commit_idx
            })
            threading.Thread(target=self._send_append_peer, args=(peer_id, port, msg, nxt), daemon=True).start()

    def _send_append_peer(self, peer_id: int, port: int, msg: Message, sent_next: int):
        resp = self._send_socket(port, msg)
        if not resp:
            return

        with self.lock:
            if self.role != "LEADER" or resp.term != self.current_term:
                return

            if resp.payload.get("success"):
                match_idx = resp.payload.get("match_index", 0)
                self.next_index[peer_id] = match_idx + 1
                self.match_index[peer_id] = match_idx
                self._calculate_highest_n_commit()
            else:
                if self.next_index.get(peer_id, 1) > 1:
                    self.next_index[peer_id] -= 1

    def _calculate_highest_n_commit(self):
        total_nodes = len(self.peers) + 1
        all_matches = sorted(list(self.match_index.values()) + [len(self.storage.log)])
        median_n = all_matches[total_nodes // 2]

        if median_n > self.commit_index:
            entry = self.storage.get_entry(median_n)
            if entry and entry["term"] == self.current_term:
                self.commit_index = median_n
                self._apply_to_state_machine()
                self.metrics["writes_committed"] += 1

    def execute_client_write(self, key: str, val: Any) -> bool:
        with self.lock:
            if not self.is_alive or self.role != "LEADER":
                return False
            new_idx = self.storage.append_entry(self.current_term, "SET", key, val)
            self._emit_log(f"📝 Appended log #{new_idx} (key='{key}'). Replicating to quorum...")

        self._broadcast_append_entries()

        # Wait for Quorum Commit (up to 1.5s)
        start_t = time.time()
        while time.time() - start_t < 1.5:
            with self.lock:
                if self.commit_index >= new_idx:
                    self._emit_log(f"✅ Quorum verified! Log #{new_idx} committed & applied.")
                    return True
            time.sleep(0.05)
        return False

    def _send_socket(self, port: int, msg: Message, timeout: float = 0.4) -> Optional[Message]:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(timeout)
                s.connect(("127.0.0.1", port))
                s.sendall(msg.serialize())
                return Message.from_socket(s)
        except Exception:
            return None

    def wipe_all(self):
        with self.lock:
            self.storage.clear()
            self.state_machine.clear()
            self.current_term = 0
            self.voted_for = None
            self.commit_index = 0
            self.last_applied = 0
            self.role = "FOLLOWER"
            self.leader_id = None
            self.blocked_peers.clear()
            self.last_heartbeat_time = time.time()
        self._emit_log("🧹 All WAL, metadata, and memory wiped clean.")
