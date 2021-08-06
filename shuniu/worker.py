import logging
import multiprocessing
import os
import signal
from collections import namedtuple
from typing import Dict

from shuniu.task import TaskApp, Task, TaskOption
from shuniu.api import shuniuRPC

Request = namedtuple("Request", ["type", "id", "worker", "src", "args", "kwargs"])
Result = namedtuple("Result", ["type", "id", "worker", "src", "result", "exception", "option"])
Event = namedtuple("Event", ["type", "id", "args", "kwargs"])


class Worker:
    def __init__(self,
                 rpc: shuniuRPC,
                 conf,
                 task_queue: multiprocessing.Queue,
                 result_queue: multiprocessing.Queue,
                 log_queue: multiprocessing.Queue,
                 event_queue: multiprocessing.Queue,
                 ):
        self.run_id = None
        self.rpc = rpc
        self.conf = conf
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.log_queue = log_queue
        self.event_queue = event_queue
        self.task_registered_map: Dict[int, Task] = {}

    def daemon(self):
        while 1:
            self.event_queue.get()



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

    def run(self):
        self.fork()
        while 1:
            self.run_id = None
            task: Request = self.task_queue.get()
            self.run_id = task.id
            call = self.task_registered_map[task.type]
            try:
                self.execute(call, task)
            except Exception as e:
                self.done(task, exception=e, option=call.option)

    def execute(self, call, task: Request):
        result = call(task.id, task.src, task.worker, task.args, task.kwargs)
        self.done(task, result=result, option=call.option)

    def done(self, task: Request, result=None, exception=None, option=None):
        self.result_queue.put(Result(task.type, task.id, task.worker, task.src, result, exception, option))

    def terminate(self):
        self


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
