from typing import Dict, Any, Tuple


class Signature:
    def __init__(self, rpc, task_name):
        self.rpc = rpc
        self.name = task_name

    def apply_async(self, *args, **kwargs) -> "AsyncResult":
        return self.rpc.apply_async(self.name, *args, **kwargs)

    def broadcast(self, *args, **kwargs) -> "AsyncResult":
        return self.rpc.broadcast(self.name, *args, **kwargs)


class Task:
    task_id = None
    wid = None
    src = None

    def __init__(
            self,
            app: "Shuniu",
            name: str,
            func: type(abs),
            conf: Dict,
            bind: bool = False,
            autoretry_for: Tuple[Exception] = None,
            ignore_result=None,
            serialization=None,
            compression=None,
            timeout=3600,
            **kwargs,
    ):
        if kwargs:
            app.logger.warning(f"Unknown parameter: {kwargs}")
        self.name = name
        self.app = app
        self.func = func
        self.bind = bind
        self.conf = conf
        self.timeout = timeout
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

    def apply_async(self, *args, **kwargs) -> "AsyncResult":
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
