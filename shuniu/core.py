#!/usr/bin/python3

import os
import re
import bz2
import gzip
import sys
import traceback
import zlib
import time
import uuid
import json
import enum
import pickle
import logging
import hashlib
import binascii
import functools
import contextlib
import threading
import urllib.parse
import queue
import multiprocessing
import multiprocessing.queues

from typing import Dict, Sequence, Any, Tuple

import bson
import requests
import requests.utils


class SerializationAlgorithm(enum.IntEnum):
    json = 1
    bson = 2
    pickle = 3


class CompressionAlgorithm(enum.IntEnum):
    none = 0
    gzip = 1
    zlib = 2
    bz2 = 3


class ErrorCode(enum.IntEnum):
    ParameterError = 400
    InsufficientPermissions = 403
    QueueEmpty = 404
    ServerInternalError = 500


class Singleton(object):
    def __init__(self, cls):
        self._cls = cls
        self._instance = {}

    def __call__(self, *args, **kwargs):
        if self._cls not in self._instance:
            self._instance[self._cls] = self._cls(*args, **kwargs)
        return self._instance[self._cls]


class EmptyData(ResourceWarning):
    pass


def __seq__():
    sid = globals().get("__increasing__cycle__", 1)
    globals()["__increasing__cycle__"] = 1 if sid >= 0xFFFF else sid + 1
    return sid


def generate_distributed_id(node_id: str, role: str) -> str:
    type_id = hashlib.md5(f"{os.getgid()}-{role}".encode()).digest()[:2]
    prefix = binascii.a2b_hex(node_id.replace("-", ""))[:4]
    ts = time.time_ns().to_bytes(8, "big")
    return str(uuid.UUID(bytes=prefix + type_id + __seq__().to_bytes(2, "big") + ts))


def decode_payload(payload: str, payload_type: int) -> Dict:
    data = binascii.a2b_base64(payload)
    coding = payload_type & 15
    compression = payload_type >> 4
    if compression == CompressionAlgorithm.gzip:
        data = gzip.decompress(data)
    elif compression == CompressionAlgorithm.zlib:
        data = zlib.decompress(data)
    elif compression == CompressionAlgorithm.bz2:
        data = bz2.decompress(data)
    elif compression == CompressionAlgorithm.none:
        pass
    else:
        raise ValueError("Unsupported compression method")
    if coding == SerializationAlgorithm.json:
        obj = json.loads(data)
    elif coding == SerializationAlgorithm.bson:
        obj = bson.loads(data)
    elif coding == SerializationAlgorithm.pickle:
        obj = pickle.loads(data)
    else:
        raise ValueError("Unsupported serialization method")
    return obj


def encode_payload(
    payload: Dict, coding: SerializationAlgorithm, compression: CompressionAlgorithm
) -> (str, int):
    if coding == SerializationAlgorithm.json:
        data = json.dumps(payload).encode()
    elif coding == SerializationAlgorithm.bson:
        data = bson.dumps(payload)
    elif coding == SerializationAlgorithm.pickle:
        data = pickle.dumps(payload)
    else:
        raise ValueError("Unsupported serialization method")
    if len(data) < 2048 or compression == CompressionAlgorithm.none:
        return binascii.b2a_base64(data).decode(), int(coding)
    if compression == CompressionAlgorithm.gzip:
        data = gzip.compress(data)
    elif compression == CompressionAlgorithm.zlib:
        data = zlib.compress(data)
    elif compression == CompressionAlgorithm.bz2:
        data = bz2.compress(data)
    else:
        raise ValueError("Unsupported compression method")
    return binascii.b2a_base64(data).decode(), int(coding + (compression << 4))


RPCDefaultConf = {
    "serialization": SerializationAlgorithm.json,
    "compression": CompressionAlgorithm.bz2,
    "ignore_result": True,
    "router": {},
}


class shuniuRPC:
    logged_in = False

    def __init__(
        self,
        url: str,
        username: str,
        password: str,
        ssl_option: Dict[str, str] = None,
        **kwargs,
    ):
        self.ssl_option = ssl_option
        self.base = urllib.parse.urljoin(url, "./rpc/")
        self.uid = uuid.UUID(username)
        self.username = username
        self.password = password
        self.task_map = {}
        self.__api__ = self.new_session()
        self.conf = {k: kwargs.get(k, v) for k, v in RPCDefaultConf.items()}

    def new_session(self):
        session = requests.Session()
        if self.ssl_option:
            if "client_cert" in self.ssl_option:
                client_cert, client_key = (
                    self.ssl_option["client_cert"],
                    self.ssl_option["client_key"],
                )
                if os.path.exists(client_key) and os.path.exists(client_cert):
                    session.cert = client_cert, client_key
                else:
                    raise ValueError("Client certificate does not exist")
            if "cacert" in self.ssl_option:
                cacert = self.ssl_option["cacert"]
                if os.path.exists(cacert):
                    session.verify = cacert
                else:
                    raise ValueError("ca certificate does not exist")
        return session

    @property
    def task_router(self):
        return {
            i[0].split(".", 1) if "." in i[0] else i[0]: i[1]
            for i in self.conf["router"].items()
        }

    def get_router(self, task: str) -> str:
        task_group, task_name = task.split(".")
        if (task_group, task_name) in self.task_router:
            return self.task_router[(task_group, task_name)]
        elif task_group in self.task_router:
            return self.task_router[task_group]
        else:
            raise ValueError("No route specified, default route missing")

    def __api_call__(self, method: str, url: str, **kwargs) -> requests.Response:
        if url != "login" and not self.logged_in:
            self.login()
        url = "./" + url.strip(".").lstrip("/")
        url = urllib.parse.urljoin(self.base, url)
        return self.__api__.request(method, url, **kwargs)

    def login(self):
        passwd = f"{self.username}:{self.password}"
        key = binascii.b2a_base64(passwd.encode()).decode()
        auth = f"Basic {key}".strip()
        with self.__api_call__("POST", "login", headers={"Authorization": auth}) as r:
            if r.ok:
                msg = r.json()
                if msg.get("code") != 0:
                    raise ValueError(
                        "Requests Error: {}".format(msg.get("msg", ""))
                    ) from None
                self.logged_in = True
                return
            else:
                raise ConnectionError(
                    "connection to rpc server error: {}".format(r.status_code)
                ) from None

    def get_task_list(self):
        with self.__api_call__("GET", "registered/task") as r:
            if r.ok and r.json().get("code") == 0:
                self.task_map.update(
                    {task["name"]: task["tid"] for task in r.json().get("data")}
                )
                self.task_map.update(
                    {task["tid"]: task["name"] for task in r.json().get("data")}
                )
            else:
                raise ValueError(
                    "Requests Error: {}".format(
                        r.ok and r.json().get("msg", "") or r.status_code
                    )
                ) from None

    def registered(self, name: str) -> int:
        if name in self.task_map:
            return self.task_map[name]
        if not isinstance(name, str) or not re.match(
            "^([a-zA-Z_]+\\.[a-zA-Z_]+)$", name
        ):
            raise ValueError("Task Name Illegal")
        with self.__api_call__("POST", f"registered/task/{name}") as r:
            if r.ok and r.json().get("code") == 0:
                tid = r.json().get("id")
                self.task_map.update({tid: name, name: tid})
                return tid
            else:
                raise ValueError(
                    "Requests Error: {}".format(
                        r.ok and r.json().get("msg", "") or r.status_code
                    )
                ) from None

    def consume(self, worker_id):
        with self.__api_call__("GET", f"/task/consume/{worker_id}") as r:
            if r.ok:
                data = r.json()
                if data["code"] == 404:
                    return EmptyData
                elif data["code"] == 0:
                    return (
                        decode_payload(data["payload"], data["type"]),
                        data["tid"],
                        data["src"],
                        data["type_id"],
                    )
            raise ValueError(
                "Requests Error: {}".format(
                    r.ok and r.json().get("msg", "") or r.status_code
                )
            ) from None

    def apply_async(
        self,
        task: str,
        *,
        args: Sequence = None,
        kwargs: Dict = None,
        queue: str = None,
        **options,
    ) -> "AsyncResult":
        if not isinstance(task, str):
            raise TypeError("task type only str")
        elif args and not isinstance(args, tuple):
            raise TypeError("args type only tuple")
        elif kwargs and not isinstance(kwargs, Dict):
            raise TypeError("args type only tuple")
        args = args or []
        kwargs = kwargs or {}
        if not queue:
            queue = self.get_router(task)
        try:
            queue = str(uuid.UUID(queue))
        except (ValueError, AttributeError):
            raise ValueError("Queue is illegal!") from None
        task_id = (
            uuid.UUID(options["task_id"]).hex
            if "task_id" in options
            else uuid.uuid5(self.uid, uuid.uuid4().hex)
        )
        task_id = str(task_id)
        payload, payload_type = encode_payload(
            {"args": list(args), "kwargs": kwargs, **options},
            coding=options.get("serialization", self.conf["serialization"]),
            compression=options.get("compression", self.conf["compression"]),
        )
        if task not in self.task_map:
            self.registered(task)
        type_id = self.task_map[task]
        data = {
            "payload": payload,
            "type": payload_type,
            "tid": task_id,
            "type_id": type_id,
            **options,
        }
        with self.__api_call__("POST", f"/task/release/{queue}", data=data) as r:
            if r.ok and r.json().get("code") == 0:
                return AsyncResult(task_id, self)
            else:
                raise ValueError(
                    "Requests Error: {}".format(
                        r.ok and r.json().get("code", "") or r.status_code
                    )
                ) from None

    def broadcast(
        self,
        task,
        destinations,
        *,
        args: Sequence = None,
        kwargs: Dict = None,
        **options,
    ) -> Dict[str, str]:
        if not isinstance(task, str):
            raise TypeError("task type only str")
        elif args and not isinstance(args, tuple):
            raise TypeError("args type only tuple")
        elif kwargs and not isinstance(kwargs, Dict):
            raise TypeError("args type only tuple")
        args = args or []
        kwargs = kwargs or {}
        if not destinations:
            raise ValueError("Destination cannot be empty")
        task_id = (
            uuid.UUID(options["task_id"]).hex
            if "task_id" in options
            else uuid.uuid5(self.uid, uuid.uuid4().hex)
        )
        task_id = str(task_id)
        payload, payload_type = encode_payload(
            {"args": list(args), "kwargs": kwargs},
            coding=options.get("serialization", self.conf["serialization"]),
            compression=options.get("compression", self.conf["compression"]),
        )
        if task not in self.task_map:
            self.registered(task)
        type_id = self.task_map[task]
        data = {
            "nid": destinations,
            "payload": payload,
            "type": payload_type,
            "tid": task_id,
            "type_id": type_id,
            **options,
        }
        with self.__api_call__("POST", "/task/broadcast/release", data=data) as r:
            if r.ok and r.json().get("code") == 0:
                return r.json()["map"]
            else:
                raise ValueError(
                    "Requests Error: {}".format(
                        r.ok and r.json().get("code", "") or r.status_code
                    )
                ) from None

    def ack(self, task_id: str, fail: bool = False, retry: bool = False):
        if retry and fail:
            raise TypeError
        method = "fail" if fail else "retry" if retry else "over"
        with self.__api_call__("PUT", f"/task/ack/{task_id}/{method}") as r:
            if r.ok and r.json().get("code") == 0:
                return
            else:
                raise ValueError(
                    "Requests Error: {}".format(
                        r.ok and r.json().get("code", "") or r.status_code
                    )
                ) from None

    def set(
        self, task_id: str, src: str, payload: Any, serialization=None, compression=None
    ):
        payload, payload_type = encode_payload(
            payload,
            coding=serialization or self.conf["serialization"],
            compression=compression or self.conf["compression"],
        )
        data = {"src": src, "payload": payload, "type": payload_type}
        with self.__api_call__("POST", f"/task/result/{task_id}", data=data) as r:
            if r.ok and r.json().get("code") == 0:
                return
            else:
                raise ValueError(
                    "Requests Error: {}".format(
                        r.ok and r.json().get("code", "") or r.status_code
                    )
                ) from None

    def get(self, task_id: str):
        with self.__api_call__("GET", f"/task/result/{task_id}") as r:
            if r.ok:
                data = r.json()
                if data["code"] == 404:
                    return EmptyData
                elif data["code"] == 0:
                    return decode_payload(data["payload"], data["type"])
            else:
                raise ValueError(
                    "Requests Error: {}".format(
                        r.ok and r.json().get("code", "") or r.status_code
                    )
                ) from None

    def revoke(self, task_id: str):
        with self.__api_call__("DELETE", f"/task/revoke/{task_id}") as r:
            if r.ok and r.json().get("code") == 0:
                return
            else:
                raise ValueError(
                    "Requests Error: {}".format(
                        r.ok and r.json().get("code", "") or r.status_code
                    )
                ) from None

    def manager(self):
        with self.__api_call__("GET", "/manager") as r:
            if r.ok:
                data = r.json()
                if data["code"] == 404:
                    return EmptyData
                elif data["code"] == 0:
                    return (
                        decode_payload(data["payload"], data["type"]),
                        data["instruction"],
                    )
            raise ValueError(
                "Requests Error: {}".format(
                    r.ok and r.json().get("msg", "") or r.status_code
                )
            ) from None

    def router(self, channel):
        with self.__api_call__("POST", "/router", data={"dst": list(channel)}) as r:
            if r.ok:
                data = r.json()
                if data["code"] == 0:
                    return data["queue"]
            raise ValueError(
                "Requests Error: {}".format(
                    r.ok and r.json().get("msg", "") or r.status_code
                )
            ) from None


@contextlib.contextmanager
def nonblocking(lock):
    locked = lock.acquire(False)
    try:
        yield locked
    finally:
        if locked:
            lock.release()


def urlparse(uri) -> Dict:
    obj = urllib.parse.urlparse(uri)
    url = urllib.parse.urlunparse(
        [obj.scheme, obj.netloc.split("@")[-1], obj.path, "", "", ""]
    )
    return {"username": obj.username, "password": obj.password, "url": url}


ShuniuDefaultConf = {
    "concurrency": multiprocessing.cpu_count(),
    "worker_enable_remote_control": True,
    "imports": [],
    "priority": 0,
    "max_retries": 3,
    "loglevel": "info",
    "logfile": None,
    "logstdout": True,
}


class WorkerLogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, "wid"):
            record.wid = "Main"
        return True


def set_logging(name, loglevel="INFO", logfile=None, logstdout=True, **kwargs):
    logger = multiprocessing.get_logger()
    logger.name = name
    logger.setLevel(loglevel.upper())
    if logfile:
        handler = logging.FileHandler(logfile)
        logger.addHandler(handler)
    if logstdout:
        handler = logging.StreamHandler()
        logger.addHandler(handler)
    logFormat = logging.Formatter("[%(levelname)s/%(name)s-%(wid)s] %(message)s")
    for handler in logger.handlers:
        handler.setFormatter(logFormat)
    logger.addFilter(WorkerLogFilter())
    return logger


@Singleton
class Shuniu:
    def __init__(self, app: str, rpc_server: str, **kwargs):
        self.app = app
        self.rpc_server = rpc_server
        conn_obj = urlparse(rpc_server)
        self.rpc = shuniuRPC(**conn_obj, **kwargs)
        self.rpc.login()
        self.rpc.get_task_list()
        self.task_registered_map: Dict[int, Task] = {}
        self.conf = {k: kwargs.get(k, v) for k, v in ShuniuDefaultConf.items()}
        self.worker_pool: Dict[int, Tuple] = {}
        self.control = {1: self.ping, 2: self.get_stats}
        self.state = {}
        self.logger = set_logging("Shuniu", **kwargs)

    def fork(self):
        fork_session = self.rpc.new_session()
        fork_session.cookies = self.rpc.__api__.cookies.copy()
        self.rpc.__api__ = fork_session

    def ping(self, *args, **kwargs):
        return True

    def get_stats(self, *args, **kwargs):
        return self.state

    def task(self, *args, **kwargs):
        if len(args) == 1 and isinstance(args, type(abs)):
            return self.registered(args[0])
        return functools.partial(self.registered, *args, **kwargs)

    def registered(self, func: type(abs), name=None, base=None, **kwargs):
        if not name:
            name = f"{self.app}.{func.__name__}"
        elif name.count(".") != 1:
            raise ValueError("task name does not meet specifications")
        type_id = self.rpc.registered(name)
        if base and issubclass(base, Task):
            worker_base = base
        else:
            worker_base = Task
        self.task_registered_map[type_id] = worker_base(
            app=self, name=name, func=func, conf=self.conf, **kwargs
        )

    def signature(self, name: str) -> "Signature":
        return Signature(self.rpc, name)

    def worker(
        self, stdin: multiprocessing.Queue, wid: int, lock: multiprocessing.Lock
    ):
        self.fork()
        task_end = set()
        self.logger.info("Fork shuniu connect")
        while 1:
            try:
                try:
                    task = stdin.get(timeout=10)
                    if not task:
                        continue
                except queue.Empty:
                    continue
                with lock:
                    kwargs, task_id, src, task_type = task
                    if task_id in task_end:
                        continue
                    task_name = self.rpc.task_map[task_type]
                    worker_class = self.task_registered_map[task_type]
                    if not worker_class.forked:
                        worker_class.__init_socket__()
                        worker_class.forked = True
                    worker_class.mock(task_id=task_id, src=src, wid=wid)
                    start_time = time.time()
                    normal = False
                    self.logger.info(
                        f"Start {self.rpc.task_map[task_type]}[{task_id}]",
                        extra={"wid": wid},
                    )
                    exc_info = None
                    try:
                        result = worker_class.run(*kwargs["args"], **kwargs["kwargs"])
                        self.rpc.ack(task_id)
                        task_end = {task_id}
                        normal = True
                    except worker_class.autoretry_for:
                        exc_info = sys.exc_info()
                        self.rpc.ack(task_id, retry=True)
                        self.logger.exception("Autoretry exception", extra={"wid": wid})
                    except Exception:
                        exc_info = sys.exc_info()
                        self.rpc.ack(task_id, fail=True)
                        self.logger.exception("Unknown exception", extra={"wid": wid})
                    runner_time = time.time() - start_time
                    if normal:
                        result = {} if isinstance(result, type(None)) else result
                        if not worker_class.ignore_result:
                            self.rpc.set(
                                task_id,
                                src,
                                payload=result,
                                serialization=worker_class.serialization,
                                compression=worker_class.compression,
                            )
                        self.logger.info(
                            f"Task {task_name}[{task_id}] succeeded in {runner_time}: {result}",
                            extra={"wid": wid},
                        )
                        worker_class.on_success()
                    else:
                        if not worker_class.ignore_result:
                            error = "".join(traceback.format_exception(*exc_info))
                            self.rpc.set(
                                task_id,
                                src,
                                payload={"__traceback__": error},
                                serialization=worker_class.serialization,
                                compression=worker_class.compression,
                            )
                        self.logger.info(
                            f"Task {task_name}[{task_id}] failure in {runner_time}",
                            extra={"wid": wid},
                        )
                        worker_class.on_failure(*exc_info)
            except Exception:
                self.logger.exception("worker collapse")

    def manager(self, kws, instruction):
        self.control[instruction](*kws["args"], **kws["kwargs"])

    def manager_worker(self):
        go_back = 1
        while 1:
            try:
                instruction = self.rpc.manager()
            except IOError:
                self.logger.error("Retry after connection loss...")
            except Exception:
                self.logger.exception("Failed to get instruction")
            else:
                if instruction != EmptyData:
                    self.manager(*instruction)
                    go_back = 1
                    continue
            time.sleep(go_back)
            go_back = 64 if go_back == 64 else go_back * 2

    def print_banners(self):
        print(
            f"""
[config]
.> app: {self.app}
.> transport: {self.rpc.base}
.> concurrency: {self.conf['concurrency']}
.> manager: {self.conf['worker_enable_remote_control']}

[tasks]"""
        )
        for tid, task in self.task_registered_map.items():
            print(f".> {self.rpc.task_map[tid]} -- ignore_result: {task.ignore_result}")

    def daemon(self):
        for wid, (worker, stdin, lock) in self.worker_pool.items():
            try:
                worker.join(timeout=0)
                if not worker.is_alive():
                    self.logger.error(f"worker {wid} is down..")
                    worker = multiprocessing.Process(
                        target=self.worker, args=(stdin, wid, lock)
                    )
                    self.worker_pool[wid] = (worker, stdin)
            except Exception:
                pass
        time.sleep(1)

    def start(self):
        globals().update({p: __import__(p) for p in self.conf["imports"]})
        if self.conf["worker_enable_remote_control"]:
            threading.Thread(target=self.manager_worker).start()
        manager = multiprocessing.Manager()
        for i in range(self.conf["concurrency"]):
            stdin = manager.Queue(maxsize=1)
            lock = manager.Lock()
            worker = multiprocessing.Process(target=self.worker, args=(stdin, i, lock))
            self.worker_pool[i] = (worker, stdin, lock)
            worker.start()
        threading.Thread(target=self.daemon).start()
        self.print_banners()
        while 1:
            for wid, (worker, stdin, lock) in self.worker_pool.items():
                with nonblocking(lock) as locked:
                    if not locked or stdin.qsize() != 0:
                        continue
                    try:
                        task = self.rpc.consume(wid)
                    except IOError:
                        self.logger.error("Retry after connection loss...")
                        time.sleep(2)
                        break
                    except Exception:
                        self.logger.exception("Failed to get task")
                        break
                    if task is not EmptyData:
                        kwargs, task_id, src, task_type = task
                        task_name = self.rpc.task_map[task_type]
                        self.logger.info(
                            f"Received task to worker-{wid}: {task_name}[{task_id}]"
                        )
                        self.state[task_name] = self.state.setdefault(task_name, 0) + 1
                        try:
                            stdin.put_nowait((kwargs, task_id, src, task_type))
                        except queue.Full:
                            self.logger.error(
                                "Failed Put {task_name}[{task_id}] to worker-{wid} Full"
                            )
                            continue
                    time.sleep(2)
            else:
                time.sleep(2)


class Signature:
    def __init__(self, rpc, task_name):
        self.rpc = rpc
        self.name = task_name

    def apply_async(self, *args, **kwargs) -> "AsyncResult":
        return self.rpc.apply_async(self.name, *args, **kwargs)

    def broadcast(self, *args, **kwargs) -> "AsyncResult":
        return self.rpc.broadcast(self.name, *args, **kwargs)


class AsyncResult:
    def __init__(self, task_id: str, rpc: shuniuRPC):
        self.rpc = rpc
        self.task_id = task_id

    def get(self) -> Any:
        result = self.rpc.get(self.task_id)
        while result == EmptyData:
            result = self.rpc.get(self.task_id)
        if isinstance(result, dict) and "__traceback__" in result:
            raise Exception(result["__traceback__"])
        return result

    def revoke(self) -> None:
        self.rpc.revoke(self.task_id)

    def __repr__(self):
        return f"<AsyncResult {self.task_id} at {hex(id(self))}>"


class Task:
    task_id = None
    wid = None
    src = None

    def __init__(
        self,
        app: Shuniu,
        name: str,
        func: type(abs),
        conf: Dict,
        bind: bool = False,
        autoretry_for: Tuple[Exception] = None,
        ignore_result=None,
        serialization=None,
        compression=None,
        **kwargs,
    ):
        if kwargs:
            app.logger.warning(f"Unknown parameter: {kwargs}")
        self.name = name
        self.app = app
        self.func = func
        self.bind = bind
        self.conf = conf
        if isinstance(ignore_result, bool):
            self.ignore_result = ignore_result
        else:
            self.ignore_result = conf.get("ignore_result", True)
        self.serialization = serialization
        self.compression = compression
        self.autoretry_for = autoretry_for or ()
        self.forked = False

    def __init_socket__(self):
        pass

    @property
    def logger(self):
        return self.app.logger

    @property
    def retry(self):
        return self.conf["max_retries"]

    def mock(self, task_id, src, wid):
        self.task_id = task_id
        self.src = src
        self.wid = wid

    def apply_async(self, *args, **kwargs) -> AsyncResult:
        return self.app.rpc.apply_async(self.name, *args, **kwargs)

    def broadcast(self, *args, **kwargs) -> Dict[str, str]:
        return self.app.rpc.broadcast(self.name, *args, **kwargs)

    def on_failure(self, exc_type, exc_value, exc_traceback):
        pass

    def on_success(self):
        pass

    def run(self, *args, **kwargs) -> Any:
        if self.bind:
            return self.func(self, *args, **kwargs)
        else:
            return self.func(*args, **kwargs)
