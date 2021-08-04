#!/usr/bin/python3
import os
import sys
import time
import logging
import traceback
import functools
import threading
import resource
import urllib.parse
import queue
import multiprocessing

from concurrent.futures import TimeoutError

from typing import Dict
from cgroups import Cgroup
from pebble import ProcessPool, ProcessExpired

from shuniu.task import Signature, Task
from shuniu.api import shuniuRPC, EmptyData
from shuniu.tools import Singleton, WorkerLogFilter


class TaskApp:
    def __init__(self, app, rpc, logger):
        self.app = app
        self.rpc = rpc
        self.logger = logger

    def signature(self, name: str) -> "Signature":
        return Signature(self.rpc, name)


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
        self.pre_request = queue.Queue()
        for i in range(self.conf["concurrency"]):
            self.pre_request.put(i)
        self.control = {1: self.kill_worker}
        self.state = {}
        self.perform = {}
        self.worker_future = {}
        self.__running__ = True
        self.logger = set_logging("Shuniu", **kwargs)

    def initializer(self):
        fork_session = self.rpc.new_session()
        fork_session.cookies = self.rpc.__api__.cookies.copy()
        self.rpc.__api__ = fork_session
        cg = Cgroup(f"worker-{os.getpid()}")
        cg.set_memory_limit(1000)
        cg.add(os.getpid())
        self.logger.info("initializer fork set limit")

    def kill_worker(self, eid, *args, **kwargs):
        future = self.worker_future.get(eid)
        if future:
            self.logger.info(f"Terminate task: {eid}")
            future.cancel()

    def get_stats(self, *args, **kwargs):
        return {"history": self.state, "run": self.perform}

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
            app=TaskApp(self.app, self.rpc, self.logger),
            name=name,
            func=func,
            conf=self.conf,
            **kwargs
        )

    def manager(self, kws, instruction):
        self.control[instruction](*kws["args"], **kws["kwargs"])

    def manager_worker(self):
        while 1:
            try:
                instruction = self.rpc.manager()
            except IOError:
                self.logger.error("Retry after connection loss...")
                time.sleep(2)
            except Exception:
                self.logger.exception("Failed to get instruction")
            else:
                if instruction != EmptyData:
                    self.manager(*instruction)

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

    def task_over(self, future, worker_class, task_id, wid, start_time, task_name, src):
        self.worker_future.pop(task_id)
        self.perform[wid] = None
        runner_time = time.time() - start_time
        try:
            try:
                result = future.result() or {}
                self.rpc.ack(task_id)
                worker_class.on_success()
                self.logger.info(
                    f"Task {task_name}[{task_id}] succeeded in {runner_time}: {result}",
                    extra={"wid": wid},
                )
                if not worker_class.ignore_result:
                    self.rpc.set(
                        task_id,
                        src,
                        payload=result,
                        serialization=worker_class.serialization,
                        compression=worker_class.compression,
                    )
            except TimeoutError:
                self.logger.exception("Task {task_name}[{task_id}] failure in {runner_time}: Timeout Kill exception",
                                      extra={"wid": wid})
                self.rpc.ack(task_id, retry=True)
                worker_class.on_failure(*sys.exc_info())
            except ProcessExpired:
                self.logger.exception("Task {task_name}[{task_id}] failure in {runner_time}: Manual kill exception",
                                      extra={"wid": wid})
                self.rpc.ack(task_id, fail=True)
                worker_class.on_failure(*sys.exc_info())
            except Exception as e:
                exc_info = sys.exc_info()
                if any(isinstance(e, ex) for ex in worker_class.autoretry_for):
                    self.logger.exception("Autoretry exception", extra={"wid": wid})
                    self.rpc.ack(task_id, retry=True)
                else:
                    self.logger.exception("Unknown exception", extra={"wid": wid})
                    self.rpc.ack(task_id, fail=True)
                worker_class.on_failure(*exc_info)
                self.logger.info(
                    f"Task {task_name}[{task_id}] failure in {runner_time}",
                    extra={"wid": wid},
                )
                if not worker_class.ignore_result:
                    error = "".join(traceback.format_exception(*exc_info))
                    self.rpc.set(
                        task_id,
                        src,
                        payload={"__traceback__": error},
                        serialization=worker_class.serialization,
                        compression=worker_class.compression,
                    )
        except Exception:
            self.logger.exception("Unknown exception")
        self.pre_request.put(wid)

    def stop(self):
        self.__running__ = False

    def start(self):
        globals().update({p: __import__(p) for p in self.conf["imports"]})
        self.print_banners()
        if self.conf["worker_enable_remote_control"]:
            threading.Thread(target=self.manager_worker).start()
        for task in self.rpc.unconfirmed():
            task_name = self.rpc.task_map[task["type_id"]]
            self.logger.info(f"Unidentified worker[{task['wid']}] task-> {task_name}[{task['tid']}]")
            self.rpc.ack(task["tid"], False, True)
        with ProcessPool(max_workers=self.conf["concurrency"], initializer=self.initializer) as pool:
            while self.__running__:
                wid = self.pre_request.get()
                while 1:
                    try:
                        task = self.rpc.consume(wid)
                    except IOError:
                        self.logger.error("Retry after connection loss...")
                        time.sleep(2)
                        continue
                    except Exception:
                        self.logger.exception("Failed to get task")
                        continue
                    if task is EmptyData:
                        continue
                    try:
                        kwargs, task_id, src, task_type = task
                        task_name = self.rpc.task_map[task_type]
                        self.logger.info(f"Received task to worker-{wid}: {task_name}[{task_id}]")
                        self.state[task_name] = self.state.setdefault(task_name, 0) + 1
                        self.perform[wid] = task_id
                        self.logger.info(f"Start {self.rpc.task_map[task_type]}[{task_id}]", extra={"wid": wid})
                        worker_class = self.task_registered_map[task_type]
                        if not worker_class.forked:
                            worker_class.__init_socket__()
                            worker_class.forked = True
                        worker_class.mock(task_id, src, wid)
                        future = pool.schedule(worker_class.run, args=kwargs["args"], kwargs=kwargs["kwargs"],
                                               timeout=worker_class.timeout)
                        self.worker_future[task_id] = future
                        callback = functools.partial(self.task_over, **{
                            "worker_class": self.task_registered_map[task_type],
                            "task_id": task_id,
                            "src": src,
                            "task_name": task_name,
                            "wid": wid,
                            "start_time": time.time()
                        })
                        future.add_done_callback(callback)
                    except Exception:
                        self.logger.exception("Send task failure")
                    break


def urlparse(uri) -> Dict:
    obj = urllib.parse.urlparse(uri)
    url = urllib.parse.urlunparse([obj.scheme, obj.netloc.split("@")[-1], obj.path, "", "", ""])
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
