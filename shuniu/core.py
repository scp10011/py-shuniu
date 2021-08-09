#!/usr/bin/python3
import os
import queue
import signal
import sys
import time
import logging
import functools
import threading
import contextlib
import urllib.parse
import multiprocessing

from typing import Dict
from shuniu.task import Task, TaskApp, Signature
from shuniu.worker import Worker
from shuniu.api import API, EmptyData
from shuniu.tools import Singleton, WorkerLogFilter
from shuniu.signal_handle import exit_handle, ExitError


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
        self.control = {1: self.kill_worker}
        self.state = {}
        self.perform = {}
        self.worker_future = {}
        self.__running__ = True
        self.worker_pool: Dict[int, tuple[Worker, queue.Queue, queue.Queue, queue.Queue]] = {}
        self.logger = set_logging("shuniu", **kwargs)
        signal.signal(signal.SIGUSR1, exit_handle)

    def kill_worker(self, eid, *args, **kwargs):
        for worker_id, task_id in self.worker_future.items():
            with contextlib.suppress(Exception):
                if task_id == eid:
                    os.kill(self.worker_pool[worker_id][0].pid, signal.SIGUSR2)
                    self.logger.info(f"Terminate task: {eid}")
                else:
                    self.logger.info(f"Terminate task does not exist: {eid}")

    def get_stats(self, *args, **kwargs):
        return {"history": self.state, "run": self.perform}

    def task(self, *args, **kwargs):
        if len(args) == 1 and isinstance(args, type(abs)):
            return self.registered(args[0])
        return functools.partial(self.registered, *args, **kwargs)

    def daemon(self, pre_request):
        while self.__running__ and not time.sleep(1):
            for worker_id, (worker, task_queue, done_queue, log_queue) in self.worker_pool.items():
                try:
                    worker.join(timeout=0)
                    if not worker.is_alive():
                        while not task_queue.empty():
                            task_queue.get()
                        self.logger.error(f"worker {worker_id} is down..")
                        self.new_worker(worker_id, task_queue, done_queue, log_queue, pre_request)
                except Exception:
                    self.logger.exception("daemon")

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

    def signature(self, name: str) -> "Signature":
        return Signature(self.rpc, name)

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
        # if not self.__running__:
        self.logger.info("Force quit")
        os.kill(os.getpid(), signal.SIGKILL)
        # else:
        #     self.logger.info("Close order received")
        #     self.__running__ = False
        #     os.kill(os.getpid(), signal.SIGUSR1)
        #     for worker_id, (worker, task_queue, *_) in self.worker_pool.items():
        #         with contextlib.suppress(Exception):
        #             os.kill(worker.pid, signal.SIGTERM)
        #         self.logger.info(f"Send stop command PID:{worker.pud} [worker-{worker_id}] SIGTERM")
        #         task_queue.put(None)
        #     time.sleep(5)
        #     os.kill(os.getpid(), signal.SIGKILL)

    def log_processing(self, log_queue):
        while 1:
            try:
                item, args, kwargs = log_queue.get()
                getattr(self.logger, item)(*args, **kwargs)
            except (ValueError, OSError):
                break
            except ExitError:
                pass
            except Exception:
                self.logger.exception("log_processing exception")

    def done_processing(self, done_queue, pre_request):
        while 1:
            try:
                worker_id = done_queue.get()
                self.perform[worker_id] = None
                pre_request.put(worker_id)
            except (ValueError, OSError):
                break
            except ExitError:
                pass
            except Exception:
                self.logger.exception("done_processing exception")

    def new_worker(self, worker_id, task_queue, done_queue, log_queue, pre_request):
        pre_request.put(worker_id)
        worker = Worker(self.registry_map, self.rpc, worker_id, task_queue, done_queue, log_queue)
        self.worker_pool[worker_id] = (worker, task_queue, done_queue, log_queue)
        worker.start()

    def start(self):
        pre_request = queue.Queue()
        manager = multiprocessing.Manager()
        threading_pool = []
        globals().update({p: __import__(p) for p in self.conf["imports"]})
        self.print_banners()
        done_queue = manager.Queue()
        log_queue = manager.Queue()
        for worker_id in range(self.conf["concurrency"]):
            self.new_worker(worker_id, manager.Queue(), done_queue, log_queue, pre_request)
        threading_pool.append(threading.Thread(target=self.daemon, args=(pre_request,)))
        threading_pool.append(threading.Thread(target=self.log_processing, args=(log_queue,)))
        threading_pool.append(threading.Thread(target=self.done_processing, args=(done_queue, pre_request,)))
        if self.conf["worker_enable_remote_control"]:
            threading_pool.append(threading.Thread(target=self.manager_worker))
        [i.start() for i in threading_pool]
        for task in self.rpc.unconfirmed():
            task_name = self.rpc.task_map[task["type_id"]]
            self.logger.info(f"Unidentified worker[{task['wid']}] task-> {task_name}[{task['tid']}]")
            self.rpc.ack(task["tid"], False, True)
        with contextlib.suppress(ExitError):
            while self.__running__:
                worker_id = pre_request.get()
                while 1:
                    try:
                        task = self.rpc.consume(worker_id)
                    except ExitError:
                        raise ExitError
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
                        if task_type not in self.registry_map:
                            self.rpc.ack(task_id, fail=True)
                            self.logger.error(f"task is not registered: {task_type}")
                            continue
                        self.worker_future[worker_id] = task_id
                        task_name = self.rpc.task_map[task_type]
                        self.logger.info(f"Received task to worker-{worker_id}: {task_name}[{task_id}]")
                        self.state[task_name] = self.state.setdefault(task_name, 0) + 1
                        self.perform[worker_id] = task_id
                        self.worker_pool[worker_id][1].put(task)
                    except ExitError:
                        raise ExitError
                    except Exception:
                        continue
                    break
        self.logger.info("Receiving and receiving directives")
        self.logger.info(f"Work unfinished: {self.perform}")
        while 1:
            if not any(self.perform.values()):
                break
            time.sleep(1)
        manager.shutdown()
        manager.join()


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
