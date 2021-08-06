import binascii
import hashlib
import uuid

from .core import Shuniu
from .api import SerializationAlgorithm, CompressionAlgorithm, EmptyData, ErrorCode
from .task import Task


def __seq__():
    sid = globals().get("__increasing__cycle__", 1)
    globals()["__increasing__cycle__"] = 1 if sid >= 0xFFFF else sid + 1
    return sid


def generate_distributed_id(node_id: str, role: str) -> str:
    type_id = hashlib.md5(f"{os.getgid()}-{role}".encode()).digest()[:2]
    prefix = binascii.a2b_hex(node_id.replace("-", ""))[:4]
    ts = time.time_ns().to_bytes(8, "big")
    return str(uuid.UUID(bytes=prefix + type_id + __seq__().to_bytes(2, "big") + ts))
