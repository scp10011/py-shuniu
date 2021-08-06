import logging
import multiprocessing
import os
import signal
from collections import namedtuple
from typing import Dict

from shuniu.task import TaskApp, Task, TaskOption
from shuniu.api import API

Request = namedtuple("Request", ["type", "id", "worker", "src", "args", "kwargs"])
Result = namedtuple("Result", ["type", "id", "worker", "src", "result", "exception", "option"])
Event = namedtuple("Event", ["type", "id", "args", "kwargs"])


class Worker:
    def __init__(self, rpc: API, conf, log_queue):
        self.run_id = None
        self.rpc = rpc
        self.conf = conf
        self.log_queue = log_queue
        self.task_registered_map: Dict[int, Task] = {}
        self.is_fork = False

    def fork(self):
        fork_session = self.rpc.new_session()
        fork_session.cookies = self.rpc.__api__.cookies.copy()
        self.rpc.__api__ = fork_session

    def registered(self, func: type(abs), name, base=None, **kwargs):
        type_id = self.rpc.registered(name)
        if base and issubclass(base, Task):
            worker_base = base
        else:
            worker_base = Task
        self.task_registered_map[type_id] = worker_base(
            app=TaskApp(self.rpc, self.log_queue),
            name=name,
            func=func,
            conf=self.conf,
            **kwargs
        )

    def run(self, task_type, task_id, wid, start_time, task_name, src, *args, **kwargs):
        if not self.is_fork:
            self.fork()
        worker_class = self.task_registered_map[task_type]
        if not worker_class.forked:
            worker_class.__init_socket__()
            worker_class.forked = True
        worker_class.mock(task_id, src, wid)
        return worker_class(*args, **kwargs)


class Scheduler:
    def __init__(self, rpc, conf, concurrent):
        self.task_queue = multiprocessing.Queue()
        self.result_queue = multiprocessing.Queue()
        self.log_queue = multiprocessing.Queue()
        self.worker = Worker(rpc, conf, self.task_queue, self.result_queue, self.log_queue)
        self.pool = multiprocessing.Pool(concurrent)
        self.worker_process: Dict[int, multiprocessing.Process] = {}

    def add_worker(self):
        self.pool.apply_async(self.worker.run)

    def terminate(self, worker):
        t = self.worker_process.get(worker)
        t.terminate()
        self.add_worker()

    def schedule(self, task_type, tid, worker, src, args, kwargs):
        request = Request(task_type, tid, worker, src, args, kwargs)
        self.task_queue.put(request)

    def daemon(self):
        while 1:
            self.pool.terminate()
