import os
import re
import bz2
import gzip
import zlib
import uuid
import json
import enum
import pickle
import binascii
import urllib.parse

from typing import Dict, Sequence, Any

import bson
import requests
import requests.utils
import requests.adapters


class API:
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
        session.mount("https://", MyHTTPAdapter(timeout=80, max_retries=3))
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
        return {i[0].split(".", 1) if "." in i[0] else i[0]: i[1] for i in self.conf["router"].items()}

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
                    raise ValueError("Requests Error: {}".format(msg.get("msg", ""))) from None
                self.logged_in = True
                return
            else:
                raise ConnectionError("connection to rpc server error: {}".format(r.status_code)) from None

    def get_task_list(self):
        with self.__api_call__("GET", "registered/task") as r:
            if r.ok and r.json().get("code") == 0:
                self.task_map.update({task["name"]: task["tid"] for task in r.json().get("data")})
                self.task_map.update({task["tid"]: task["name"] for task in r.json().get("data")})
            else:
                raise ValueError(
                    "Requests Error: {}".format(r.ok and r.json().get("msg", "") or r.status_code)
                ) from None

    def registered(self, name: str) -> int:
        if name in self.task_map:
            return self.task_map[name]
        if not isinstance(name, str) or not re.match("^([a-zA-Z_]+\\.[a-zA-Z_]+)$", name):
            raise ValueError("Task Name Illegal")
        with self.__api_call__("POST", f"registered/task/{name}") as r:
            if r.ok and r.json().get("code") == 0:
                tid = r.json().get("id")
                self.task_map.update({tid: name, name: tid})
                return tid
            else:
                raise ValueError(
                    "Requests Error: {}".format(r.ok and r.json().get("msg", "") or r.status_code)
                ) from None

    def unconfirmed(self):
        with self.__api_call__("GET", "/task/unconfirmed") as r:
            if r.ok:
                data = r.json()
                if data["code"] == 0:
                    return data["tasks"]
            raise ValueError("Requests Error: {}".format(r.ok and r.json().get("msg", "") or r.status_code)) from None

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
            raise ValueError("Requests Error: {}".format(r.ok and r.json().get("msg", "") or r.status_code)) from None

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
        task_id = uuid.UUID(options["task_id"]).hex if "task_id" in options else uuid.uuid5(self.uid, uuid.uuid4().hex)
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
                    "Requests Error: {}".format(r.ok and r.json().get("code", "") or r.status_code)
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
        task_id = uuid.UUID(options["task_id"]).hex if "task_id" in options else uuid.uuid5(self.uid, uuid.uuid4().hex)
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
                    "Requests Error: {}".format(r.ok and r.json().get("code", "") or r.status_code)
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
                    "Requests Error: {}".format(r.ok and r.json().get("code", "") or r.status_code)
                ) from None

    def set(self, task_id: str, src: str, payload: Any, serialization=None, compression=None, **kwargs):
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
                    "Requests Error: {}".format(r.ok and r.json().get("code", "") or r.status_code)
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
                    "Requests Error: {}".format(r.ok and r.json().get("code", "") or r.status_code)
                ) from None

    def revoke(self, task_id: str):
        with self.__api_call__("DELETE", f"/task/revoke/{task_id}") as r:
            if r.ok and r.json().get("code") == 0:
                return
            else:
                raise ValueError(
                    "Requests Error: {}".format(r.ok and r.json().get("code", "") or r.status_code)
                ) from None

    def pause(self, task_id: str):
        with self.__api_call__("PUT", f"/task/pause/{task_id}") as r:
            if r.ok and r.json().get("code") == 0:
                return
            else:
                raise ValueError(
                    "Requests Error: {}".format(r.ok and r.json().get("code", "") or r.status_code)
                ) from None

    def restore(self, task_id: str):
        with self.__api_call__("PUT", f"/task/restore/{task_id}") as r:
            if r.ok and r.json().get("code") == 0:
                return
            else:
                raise ValueError(
                    "Requests Error: {}".format(r.ok and r.json().get("code", "") or r.status_code)
                ) from None

    def many_revoke(self, tids):
        with self.__api_call__("POST", f"/task/revoke", data={"eid": tids}) as r:
            if r.ok and r.json().get("code") == 0:
                return r.json().get("state")
            else:
                raise ValueError(
                    "Requests Error: {}".format(r.ok and r.json().get("code", "") or r.status_code)
                ) from None

    def many_pause(self, tids):
        with self.__api_call__("POST", f"/task/pause", data={"eid": tids}) as r:
            if r.ok and r.json().get("code") == 0:
                return r.json().get("state")
            else:
                raise ValueError(
                    "Requests Error: {}".format(r.ok and r.json().get("code", "") or r.status_code)
                ) from None

    def many_restore(self, tids):
        with self.__api_call__("POST", f"/task/restore", data={"eid": tids}) as r:
            if r.ok and r.json().get("code") == 0:
                return r.json().get("state")
            else:
                raise ValueError(
                    "Requests Error: {}".format(r.ok and r.json().get("code", "") or r.status_code)
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
            raise ValueError("Requests Error: {}".format(r.ok and r.json().get("msg", "") or r.status_code)) from None

    def router(self, channel):
        with self.__api_call__("POST", "/router", data={"dst": list(channel)}) as r:
            if r.ok:
                data = r.json()
                if data["code"] == 0:
                    return data["queue"]
            raise ValueError("Requests Error: {}".format(r.ok and r.json().get("msg", "") or r.status_code)) from None


class SerializationAlgorithm(enum.IntEnum):
    json = 1
    bson = 2
    pickle = 3


class CompressionAlgorithm(enum.IntEnum):
    none = 0
    gzip = 1
    zlib = 2
    bz2 = 3


class EmptyData(ResourceWarning):
    pass


class AsyncResult:
    def __init__(self, task_id: str, rpc: API):
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

    def pause(self) -> None:
        self.rpc.pause(self.task_id)

    def restore(self) -> None:
        self.rpc.restore(self.task_id)

    def __repr__(self):
        return f"<AsyncResult {self.task_id} at {hex(id(self))}>"


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


def encode_payload(payload: Dict, coding: SerializationAlgorithm, compression: CompressionAlgorithm) -> (str, int):
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


class MyHTTPAdapter(requests.adapters.HTTPAdapter):
    def __init__(self, timeout=None, *args, **kwargs):
        self.timeout = timeout
        super(MyHTTPAdapter, self).__init__(*args, **kwargs)

    def send(self, *args, **kwargs):
        kwargs.update(timeout=getattr(self, "timeout", 80))
        return super(MyHTTPAdapter, self).send(*args, **kwargs)


class ErrorCode(enum.IntEnum):
    ParameterError = 400
    InsufficientPermissions = 403
    QueueEmpty = 404
    ServerInternalError = 500
