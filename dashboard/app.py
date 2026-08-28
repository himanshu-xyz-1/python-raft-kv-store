import tkinter as tk
from tkinter import messagebox
import queue
from typing import Dict
from raft.node import RaftNode
from .theme import ThemeManager

class ClusterDashboard:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Raft Consensus Distributed Key-Value Engine V3")
        self.root.geometry("1240x880")
        self.root.minsize(1050, 720)

        self.theme_mgr = ThemeManager()
        self.event_bus = queue.Queue()

        self.nodes: Dict[int, RaftNode] = {}
        self._init_cluster()
        self._setup_ui()
        self._apply_theme()

        self.root.after(100, self._process_events)
        self.root.after(300, self._update_ui_state)

    def _init_cluster(self):
        ports = {1: 8001, 2: 8002, 3: 8003}
        for node_id, port in ports.items():
            peers = {p_id: p_port for p_id, p_port in ports.items() if p_id != node_id}
            self.nodes[node_id] = RaftNode(node_id, port, peers, self.event_bus)
            self.nodes[node_id].start()

    def _setup_ui(self):
        self.root_frame = tk.Frame(self.root)
        self.root_frame.pack(fill="both", expand=True)

        # Header
        self.header = tk.Frame(self.root_frame)
        self.header.pack(fill="x", padx=20, pady=(15, 8))

        self.title_lbl = tk.Label(self.header, text="⚡ Raft Distributed KV Storage Engine V3", font=("Segoe UI", 15, "bold"))
        self.title_lbl.pack(side="left")

        btn_group = tk.Frame(self.header)
        btn_group.pack(side="right")

        self.clear_btn = tk.Button(btn_group, text="🧹 Wipe All Logs & Data", font=("Segoe UI", 9, "bold"),
                                   relief="flat", cursor="hand2", padx=12, pady=5, command=self._wipe_cluster)
        self.clear_btn.pack(side="left", padx=(0, 10))

        self.theme_btn = tk.Button(btn_group, text="🌙 Dark Mode", font=("Segoe UI", 9, "bold"),
                                   relief="flat", cursor="hand2", padx=12, pady=5, command=self._toggle_theme)
        self.theme_btn.pack(side="left")

        # Top Grid: Node Status Cards
        self.nodes_container = tk.Frame(self.root_frame)
        self.nodes_container.pack(fill="x", padx=20, pady=6)

        self.node_widgets = {}
        for i, node_id in enumerate(self.nodes.keys()):
            card = tk.Frame(self.nodes_container, bd=1, relief="solid", padx=14, pady=12)
            card.pack(side="left", fill="both", expand=True, padx=(0 if i==0 else 10, 0))

            lbl_id = tk.Label(card, text=f"Node {node_id} (:800{node_id})", font=("Segoe UI", 12, "bold"))
            lbl_id.pack(anchor="w")

            lbl_role = tk.Label(card, text="ROLE: FOLLOWER", font=("Segoe UI", 9, "bold"), padx=6, pady=2)
            lbl_role.pack(anchor="w", pady=(6, 4))

            lbl_term = tk.Label(card, text="Term: 0 | Commit: 0 | Applied: 0", font=("Segoe UI", 9))
            lbl_term.pack(anchor="w", pady=(2, 6))

            btn_box = tk.Frame(card)
            btn_box.pack(fill="x")

            btn_crash = tk.Button(btn_box, text="Crash Node", font=("Segoe UI", 9, "bold"), relief="flat",
                                  cursor="hand2", padx=8, pady=4, command=lambda nid=node_id: self._toggle_node(nid))
            btn_crash.pack(side="left", fill="x", expand=True, padx=(0, 4))

            btn_partition = tk.Button(btn_box, text="Partition", font=("Segoe UI", 9, "bold"), relief="flat",
                                      cursor="hand2", padx=8, pady=4, command=lambda nid=node_id: self._toggle_partition(nid))
            btn_partition.pack(side="left", fill="x", expand=True)

            self.node_widgets[node_id] = {
                "card": card, "lbl_id": lbl_id, "lbl_role": lbl_role,
                "lbl_term": lbl_term, "btn_crash": btn_crash, "btn_partition": btn_partition
            }

        # Middle: Input Operations & Memory Store
        self.mid_frame = tk.Frame(self.root_frame)
        self.mid_frame.pack(fill="x", padx=20, pady=6)

        self.input_card = tk.Frame(self.mid_frame, bd=1, relief="solid", padx=14, pady=12)
        self.input_card.pack(side="left", fill="both", expand=True, padx=(0, 10))

        tk.Label(self.input_card, text="Client Data Operations", font=("Segoe UI", 11, "bold")).pack(anchor="w", pady=(0, 6))

        r1 = tk.Frame(self.input_card)
        r1.pack(fill="x", pady=2)
        tk.Label(r1, text="Key:", width=6, anchor="w", font=("Segoe UI", 10)).pack(side="left")
        self.entry_key = tk.Entry(r1, font=("Segoe UI", 10))
        self.entry_key.pack(side="left", fill="x", expand=True)

        r2 = tk.Frame(self.input_card)
        r2.pack(fill="x", pady=2)
        tk.Label(r2, text="Value:", width=6, anchor="w", font=("Segoe UI", 10)).pack(side="left")
        self.entry_val = tk.Entry(r2, font=("Segoe UI", 10))
        self.entry_val.pack(side="left", fill="x", expand=True)

        btn_r = tk.Frame(self.input_card)
        btn_r.pack(fill="x", pady=(8, 0))

        self.btn_set = tk.Button(btn_r, text="Set Key", font=("Segoe UI", 9, "bold"), relief="flat",
                                 cursor="hand2", padx=10, pady=4, command=self._handle_set)
        self.btn_set.pack(side="left", fill="x", expand=True, padx=(0, 4))

        self.btn_get = tk.Button(btn_r, text="Get Key", font=("Segoe UI", 9, "bold"), relief="flat",
                                 cursor="hand2", padx=10, pady=4, command=self._handle_get)
        self.btn_get.pack(side="left", fill="x", expand=True)

        self.store_card = tk.Frame(self.mid_frame, bd=1, relief="solid", padx=14, pady=12)
        self.store_card.pack(side="right", fill="both", expand=True)

        tk.Label(self.store_card, text="Committed State Machine View", font=("Segoe UI", 11, "bold")).pack(anchor="w", pady=(0, 4))
        self.store_listbox = tk.Listbox(self.store_card, height=4, font=("Consolas", 10), relief="flat")
        self.store_listbox.pack(fill="both", expand=True)

        # Bottom 3-Column Node Consoles
        self.console_container = tk.Frame(self.root_frame)
        self.console_container.pack(fill="both", expand=True, padx=20, pady=(6, 12))

        self.node_consoles = {}
        for i, node_id in enumerate(self.nodes.keys()):
            col = tk.Frame(self.console_container, bd=1, relief="solid", padx=10, pady=8)
            col.pack(side="left", fill="both", expand=True, padx=(0 if i==0 else 8, 0))

            lbl_c = tk.Label(col, text=f"📋 Node {node_id} Activity Stream", font=("Segoe UI", 10, "bold"))
            lbl_c.pack(anchor="w", pady=(0, 4))

            txt = tk.Text(col, font=("Consolas", 9), relief="flat", wrap="word", height=9)
            txt.pack(fill="both", expand=True)

            self.node_consoles[node_id] = {"col": col, "lbl": lbl_c, "txt": txt}

    def _toggle_theme(self):
        is_dark = self.theme_mgr.toggle()
        self.theme_btn.config(text="☀️ Light Mode" if is_dark else "🌙 Dark Mode")
        self._apply_theme()

    def _apply_theme(self):
        c = self.theme_mgr.current
        self.root.configure(bg=c["bg_root"])
        self.root_frame.configure(bg=c["bg_root"])
        self.header.configure(bg=c["bg_root"])
        self.title_lbl.configure(bg=c["bg_root"], fg=c["text_primary"])
        self.clear_btn.configure(bg=c["btn_clear_bg"], fg=c["btn_clear_fg"])
        self.theme_btn.configure(bg=c["bg_secondary"], fg=c["text_primary"])

        self.nodes_container.configure(bg=c["bg_root"])
        for _, w in self.node_widgets.items():
            w["card"].configure(bg=c["bg_surface"], highlightbackground=c["border"])
            w["lbl_id"].configure(bg=c["bg_surface"], fg=c["text_primary"])
            w["lbl_term"].configure(bg=c["bg_surface"], fg=c["text_secondary"])

        self.mid_frame.configure(bg=c["bg_root"])
        self.input_card.configure(bg=c["bg_surface"], highlightbackground=c["border"])
        for widget in self.input_card.winfo_children():
            if isinstance(widget, tk.Label):
                widget.configure(bg=c["bg_surface"], fg=c["text_primary"])
            elif isinstance(widget, tk.Frame):
                widget.configure(bg=c["bg_surface"])
                for sub in widget.winfo_children():
                    if isinstance(sub, tk.Label):
                        sub.configure(bg=c["bg_surface"], fg=c["text_secondary"])
                    elif isinstance(sub, tk.Entry):
                        sub.configure(bg=c["bg_secondary"], fg=c["text_primary"], relief="flat")

        self.btn_set.configure(bg=c["accent"], fg="#ffffff")
        self.btn_get.configure(bg=c["bg_secondary"], fg=c["text_primary"])

        self.store_card.configure(bg=c["bg_surface"], highlightbackground=c["border"])
        for widget in self.store_card.winfo_children():
            if isinstance(widget, tk.Label):
                widget.configure(bg=c["bg_surface"], fg=c["text_primary"])

        self.store_listbox.configure(bg=c["bg_secondary"], fg=c["text_primary"])

        self.console_container.configure(bg=c["bg_root"])
        for _, w in self.node_consoles.items():
            w["col"].configure(bg=c["bg_surface"], highlightbackground=c["border"])
            w["lbl"].configure(bg=c["bg_surface"], fg=c["text_primary"])
            w["txt"].configure(bg=c["console_bg"], fg=c["console_text"])

    def _toggle_node(self, node_id: int):
        node = self.nodes[node_id]
        if node.is_alive:
            node.crash()
        else:
            node.recover()

    def _toggle_partition(self, node_id: int):
        node = self.nodes[node_id]
        is_isolated = len(node.blocked_peers) > 0
        for peer_id in node.peers.keys():
            node.set_partition(peer_id, not is_isolated)
            self.nodes[peer_id].set_partition(node_id, not is_isolated)
        status = "RE-CONNECTED" if is_isolated else "ISOLATED"
        node._emit_log(f"🌐 Partition state changed: {status}")

    def _wipe_cluster(self):
        if not messagebox.askyesno("Confirm", "Wipe all WAL logs, persistent metadata, and state machines?"):
            return
        for node in self.nodes.values():
            node.wipe_all()
        for w in self.node_consoles.values():
            w["txt"].delete("1.0", tk.END)
        self.store_listbox.delete(0, tk.END)

    def _handle_set(self):
        k = self.entry_key.get().strip()
        v = self.entry_val.get().strip()
        if not k or not v:
            messagebox.showwarning("Warning", "Enter both Key and Value!")
            return
        leader = next((n for n in self.nodes.values() if n.is_alive and n.role == "LEADER"), None)
        if not leader:
            messagebox.showerror("Error", "No active Leader available!")
            return
        if leader.execute_client_write(k, v):
            self.entry_key.delete(0, tk.END)
            self.entry_val.delete(0, tk.END)
        else:
            messagebox.showwarning("Write Blocked", "Quorum could not be achieved (Quorum Safety).")

    def _handle_get(self):
        k = self.entry_key.get().strip()
        if not k:
            return
        node = next((n for n in self.nodes.values() if n.is_alive), None)
        if not node:
            return
        val = node.state_machine.get(k)
        messagebox.showinfo("Read", f"Key: {k}\nValue: {val}")

    def _process_events(self):
        while not self.event_bus.empty():
            try:
                evt = self.event_bus.get_nowait()
                if evt["type"] == "NODE_LOG":
                    nid = evt["node_id"]
                    if nid in self.node_consoles:
                        box = self.node_consoles[nid]["txt"]
                        box.insert(tk.END, f"[{evt['timestamp']}] {evt['text']}\n")
                        box.see(tk.END)
            except queue.Empty:
                break
        self.root.after(100, self._process_events)

    def _update_ui_state(self):
        c = self.theme_mgr.current
        for nid, node in self.nodes.items():
            w = self.node_widgets[nid]
            if not node.is_alive:
                role_t = "STATUS: DEAD"
                role_c = c["dead"]
                btn_t = "Recover"
                btn_c = c["leader"]
            else:
                role_t = f"ROLE: {node.role}"
                btn_t = "Crash"
                btn_c = c["dead"]
                role_c = c["leader"] if node.role == "LEADER" else (c["candidate"] if node.role == "CANDIDATE" else c["follower"])

            w["lbl_role"].config(text=role_t, fg="#ffffff", bg=role_c)
            w["lbl_term"].config(text=f"Term: {node.current_term} | Commit: {node.commit_index} | Appld: {node.last_applied}")
            w["btn_crash"].config(text=btn_t, bg=btn_c, fg="#ffffff")

            part_text = "Unpartition" if len(node.blocked_peers) > 0 else "Partition"
            w["btn_partition"].config(text=part_text, bg=c["bg_secondary"], fg=c["text_primary"])

        # Update State Machine Display from active nodes
        active_node = next((n for n in self.nodes.values() if n.is_alive and n.role == "LEADER"), None)
        self.store_listbox.delete(0, tk.END)
        if active_node:
            for k, v in active_node.state_machine.get_all().items():
                self.store_listbox.insert(tk.END, f" {k}  =>  {v}")
        self.root.after(300, self._update_ui_state)
