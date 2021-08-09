import logging
import multiprocessing
from typing import Dict, Any, Tuple


class Signature:
    def __init__(self, rpc, task_name):
        self.rpc = rpc
        self.name = task_name

    def apply_async(self, *args, **kwargs) -> "AsyncResult":
        return self.rpc.apply_async(self.name, *args, **kwargs)

    def broadcast(self, *args, **kwargs) -> "AsyncResult":
        return self.rpc.broadcast(self.name, *args, **kwargs)


class TaskApp:
    def __init__(self, rpc, log_level):
        self.app = "worker"
        self.rpc = rpc
        self.__logger__ = None
        self.log_level = log_level

    def signature(self, name: str) -> Signature:
        return Signature(self.rpc, name)

    @property
    def logger(self) -> logging.Logger:
        if not self.__logger__:
            self.__logger__ = logging.getLogger("Worker")
            self.__logger__.setLevel(self.log_level.upper())
        return self.__logger__


class TaskOption:
    autoretry_for: Tuple[Exception] = None
    ignore_result = None
    serialization = None
    compression = None
    timeout = 3600
    bind: bool = False

    def __init__(self, conf,
                 autoretry_for: Tuple[Exception] = None,
                 bind=True,
                 ignore_result=None,
                 serialization=None,
                 compression=None,
                 timeout=3600,
                 **kwargs):
        self.bind = bind
        self.timeout = timeout
        if isinstance(ignore_result, bool):
            self.ignore_result = ignore_result
        else:
            self.ignore_result = conf.get("ignore_result", True)
        self.serialization = serialization
        self.compression = compression
        self.autoretry_for = autoretry_for or ()
        self.unknown = kwargs

    def keys(self):
        return ["ignore_result", "compression", "serialization", "autoretry_for", "bind", "timeout", *self.unknown]

    def items(self):
        return {**self, **self.unknown}

    def values(self):
        return [getattr(self, i) for i in self.keys()] + list(self.unknown.values())

    def __getitem__(self, item):
        if item in self.unknown:
            return self.unknown[item]
        return getattr(self, item)


class Task:
    task_id = None
    wid = None
    src = None

    def __init__(
            self,
            app: TaskApp,
            name: str,
            func: type(abs),
            conf: Dict,
            **kwargs,
    ):
        self.name = name
        self.app = app
        self.func = func
        self.conf = conf
        self.option = TaskOption(self.conf, **kwargs)
        if self.option.unknown:
            app.logger.warning(f"Unknown parameter: {self.option.unknown}")
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

    def apply_async(self, *args, **kwargs) -> "AsyncResult":
        return self.app.rpc.apply_async(self.name, *args, **kwargs)

    def broadcast(self, *args, **kwargs) -> Dict[str, str]:
        return self.app.rpc.broadcast(self.name, *args, **kwargs)

    def on_failure(self, exc_type, exc_value, exc_traceback):
        pass

    def on_success(self):
        pass

    def __call__(self, args, kwargs):
        if self.option.bind:
            return self.func(self, *args, **kwargs)
        else:
            return self.func(*args, **kwargs)
