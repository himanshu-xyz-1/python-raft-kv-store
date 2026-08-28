import json
import struct
import socket
from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional

class ProtocolError(Exception):
    pass

@dataclass
class Message:
    sender_id: int
    msg_type: str  # REQUEST_VOTE, VOTE_ACK, APPEND_ENTRIES, APPEND_ACK, CLIENT_REQ, CLIENT_RESP
    term: int
    payload: Dict[str, Any]

    def serialize(self) -> bytes:
        data = json.dumps(asdict(self)).encode('utf-8')
        length_prefix = struct.pack('!I', len(data))
        return length_prefix + data

    @classmethod
    def from_socket(cls, sock: socket.socket) -> Optional['Message']:
        try:
            raw_len = cls._recv_exact(sock, 4)
            if not raw_len:
                return None
            msg_len = struct.unpack('!I', raw_len)[0]
            if msg_len > 10 * 1024 * 1024:  # 10MB safety cap
                raise ProtocolError("Frame size exceeded maximum limit")
            raw_payload = cls._recv_exact(sock, msg_len)
            if not raw_payload:
                return None
            data = json.loads(raw_payload.decode('utf-8'))
            return cls(
                sender_id=data['sender_id'],
                msg_type=data['msg_type'],
                term=data['term'],
                payload=data.get('payload', {})
            )
        except (socket.timeout, ConnectionResetError, BrokenPipeError, ProtocolError):
            return None
        except Exception:
            return None

    @staticmethod
    def _recv_exact(sock: socket.socket, num_bytes: int) -> Optional[bytes]:
        buffer = bytearray()
        while len(buffer) < num_bytes:
            packet = sock.recv(min(num_bytes - len(buffer), 4096))
            if not packet:
                return None
            buffer.extend(packet)
        return bytes(buffer)
