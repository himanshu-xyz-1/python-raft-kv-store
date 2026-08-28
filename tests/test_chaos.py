import os
import shutil
import time
import queue
import unittest
from raft.node import RaftNode
from raft.storage import RaftStorage

class TestRaftChaosSuite(unittest.TestCase):
    _port_counter = 9300

    def setUp(self):
        # 1. Isolated test storage directory
        self.test_dir = f"test_cluster_data_{int(time.time() * 1000)}"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir, ignore_errors=True)
        os.makedirs(self.test_dir, exist_ok=True)

        self.bus = queue.Queue()
        
        # 2. Dynamic port assignment per test to avoid Linux TIME_WAIT conflicts
        TestRaftChaosSuite._port_counter += 10
        base_port = TestRaftChaosSuite._port_counter
        self.ports = {1: base_port + 1, 2: base_port + 2, 3: base_port + 3}
        self.nodes = {}

        for nid, p in self.ports.items():
            peers = {pid: prt for pid, prt in self.ports.items() if pid != nid}
            node = RaftNode(nid, p, peers, self.bus, election_min_ms=400, election_max_ms=800, heartbeat_ms=80)
            
            # Reassign clean isolated test storage
            node.storage = RaftStorage(nid, storage_dir=self.test_dir)
            node.current_term = node.storage.current_term
            node.voted_for = node.storage.voted_for
            
            node.start()
            self.nodes[nid] = node

        # Ensure cluster elects a stable leader before test starts
        self._wait_for_leader(timeout=6.0)

    def tearDown(self):
        for n in self.nodes.values():
            n.crash()
        time.sleep(0.3)
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir, ignore_errors=True)

    def _wait_for_leader(self, timeout: float = 6.0):
        start = time.time()
        while time.time() - start < timeout:
            leader = next((n for n in self.nodes.values() if n.is_alive and n.role == "LEADER"), None)
            if leader:
                return leader
            time.sleep(0.1)
        return None

    def test_01_leader_election_and_quorum_write(self):
        leader = self._wait_for_leader()
        self.assertIsNotNone(leader, "Cluster must elect a leader")
        
        ok = leader.execute_client_write("player", "himanshu")
        self.assertTrue(ok, "Write must succeed under full quorum")

        time.sleep(0.4)
        for n in self.nodes.values():
            self.assertEqual(n.state_machine.get("player"), "himanshu")

    def test_02_quorum_loss_blocks_write(self):
        leader = self._wait_for_leader()
        self.assertIsNotNone(leader, "Leader must be present initially")

        # Crash 2 followers -> Quorum lost (1/3 remaining)
        followers = [n for n in self.nodes.values() if n.node_id != leader.node_id]
        for f in followers:
            f.crash()

        time.sleep(0.3)
        ok = leader.execute_client_write("blocked_key", "val")
        self.assertFalse(ok, "Write must fail when quorum is lost")

    def test_03_network_partition_and_reconciliation(self):
        leader = self._wait_for_leader()
        self.assertIsNotNone(leader, "Leader must be present")
        
        isolated_follower = next(n for n in self.nodes.values() if n.node_id != leader.node_id)

        # Isolate follower
        isolated_follower.set_partition(leader.node_id, True)
        leader.set_partition(isolated_follower.node_id, True)

        # Write to majority partition (2/3 quorum holds)
        ok = leader.execute_client_write("cluster_state", "active")
        self.assertTrue(ok, "Majority partition must commit writes")

        # Heal network partition
        isolated_follower.set_partition(leader.node_id, False)
        leader.set_partition(isolated_follower.node_id, False)

        # Wait for heartbeats to reconcile missed logs
        time.sleep(1.2)
        self.assertEqual(isolated_follower.state_machine.get("cluster_state"), "active",
                         "Healed node must catch up to committed log entries")

if __name__ == "__main__":
    unittest.main()