#!/usr/bin/python3
import contextlib
import time
import logging
import functools
import threading
import urllib.parse
import queue
import multiprocessing

from typing import Dict
from shuniu.task import Task, TaskApp
from shuniu.worker import Worker
from shuniu.api import API, EmptyData
from shuniu.tools import Singleton, WorkerLogFilter


def initializer():
    print("初始化worker设置")


@Singleton
class Shuniu:
    def __init__(self, app: str, rpc_server: str, **kwargs):
        self.app = app
        self.rpc_server = rpc_server
        conn_obj = urlparse(rpc_server)
        self.rpc = API(**conn_obj, **kwargs)
        self.rpc.login()
        self.rpc.get_task_list()
        self.conf = {k: kwargs.get(k, v) for k, v in ShuniuDefaultConf.items()}
        self.registry_map: Dict[int, Task] = {}
        self.pre_request = queue.Queue()
        for i in range(self.conf["concurrency"]):
            self.pre_request.put(i)
        self.control = {1: self.kill_worker}
        self.state = {}
        self.perform = {}
        self.worker_future = {}
        self.__running__ = True
        self.worker_pool = {}
        self.log_queue = multiprocessing.Queue()
        self.done_queue = multiprocessing.Queue()
        self.logger = set_logging("shuniu", **kwargs)

    def kill_worker(self, eid, *args, **kwargs):
        for worker_id, task_id in self.worker_future.items():
            if task_id == eid:
                self.logger.info(f"Terminate task: {eid}")
                self.worker_pool[worker_id][0].terminate()
            else:
                self.logger.info(f"Terminate task does not exist: {eid}")

    def get_stats(self, *args, **kwargs):
        return {"history": self.state, "run": self.perform}

    def task(self, *args, **kwargs):
        if len(args) == 1 and isinstance(args, type(abs)):
            return self.registered(args[0])
        return functools.partial(self.registered, *args, **kwargs)

    def daemon(self):
        while self.__running__:
            for worker_id, (worker, stdin) in self.worker_pool.items():
                with contextlib.suppress(Exception):
                    worker.join(timeout=0)
                    if not worker.is_alive():
                        self.logger.error(f"worker {worker_id} is down..")
                        task_queue = multiprocessing.Queue()
                        worker = Worker(self.registry_map, self.rpc, worker_id, task_queue, self.done_queue,
                                        self.log_queue)
                        self.worker_pool[worker_id] = (worker, task_queue)
                        worker.start()
            time.sleep(1)

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
        app = TaskApp(self.rpc, self.conf["loglevel"])
        self.registry_map[type_id] = worker_base(app=app, name=name, func=func, conf=self.conf, **kwargs)

    def manager(self, kws, instruction):
        self.control[instruction](*kws["args"], **kws["kwargs"])

    def manager_worker(self):
        while self.__running__:
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
        for tid, task in self.registry_map.items():
            print(f".> {self.rpc.task_map[tid]} -- ignore_result: {task.option.ignore_result}")

    def stop(self):
        self.logger.info("Close order received")
        self.__running__ = False

    def log_processing(self):
        while self.__running__:
            with contextlib.suppress(Exception):
                item, args, kwargs = self.log_queue.get()
                getattr(self.logger, item)(*args, **kwargs)

    def done_processing(self):
        while self.__running__:
            done = self.done_queue.get()
            self.perform[done] = None
            self.pre_request.put(done)

    def start(self):
        threading_pool = []
        globals().update({p: __import__(p) for p in self.conf["imports"]})
        self.print_banners()
        for worker_id in range(self.conf["concurrency"]):
            task_queue = multiprocessing.Queue()
            worker = Worker(self.registry_map, self.rpc, worker_id, task_queue, self.done_queue, self.log_queue)
            self.worker_pool[worker_id] = (worker, task_queue)
            worker.start()
        threading_pool.append(threading.Thread(target=self.done_processing))
        threading_pool.append(threading.Thread(target=self.log_processing))
        threading_pool.append(threading.Thread(target=self.daemon))
        if self.conf["worker_enable_remote_control"]:
            threading_pool.append(threading.Thread(target=self.manager_worker))
        [i.start() for i in threading_pool]
        for task in self.rpc.unconfirmed():
            task_name = self.rpc.task_map[task["type_id"]]
            self.logger.info(f"Unidentified worker[{task['wid']}] task-> {task_name}[{task['tid']}]")
            self.rpc.ack(task["tid"], False, True)
        while self.__running__:
            worker_id = self.pre_request.get()
            while 1:
                try:
                    task = self.rpc.consume(worker_id)
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
                    self.worker_future[worker_id] = task_id
                    task_name = self.rpc.task_map[task_type]
                    self.logger.info(f"Received task to worker-{worker_id}: {task_name}[{task_id}]")
                    self.state[task_name] = self.state.setdefault(task_name, 0) + 1
                    self.perform[worker_id] = task_id
                    self.worker_pool[worker_id][1].put(task)
                except Exception:
                    continue
                break
        self.logger.info("Receiving and receiving directives")
        while 1:
            if not any(self.perform.values()):
                break
            time.sleep(1)
        self.log_queue.close()
        self.done_queue.close()
        self.logger.info("Completely leave the ownership mission")
        [i.join() for i in threading_pool]
        for worker_id, (worker, task_queue) in self.worker_pool.items():
            self.logger.info(f"Terminate the process: {worker_id}")
            task_queue.close()
            worker.close()


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
