import binascii
import hashlib
import logging
import os
import time
import uuid


class Singleton(object):
    def __init__(self, cls):
        self._cls = cls
        self._instance = {}

    def __call__(self, *args, **kwargs):
        if self._cls not in self._instance:
            self._instance[self._cls] = self._cls(*args, **kwargs)
        return self._instance[self._cls]


class WorkerLogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, "wid"):
            record.wid = "Main"
        return True


def __seq__():
    sid = globals().get("__increasing__cycle__", 1)
    globals()["__increasing__cycle__"] = 1 if sid >= 0xFFFF else sid + 1
    return sid


def generate_distributed_id(node_id: str, role: str) -> str:
    type_id = hashlib.md5(f"{os.getgid()}-{role}".encode()).digest()[:2]
    prefix = binascii.a2b_hex(node_id.replace("-", ""))[:4]
    ts = time.time_ns().to_bytes(8, "big")
    return str(uuid.UUID(bytes=prefix + type_id + __seq__().to_bytes(2, "big") + ts))
