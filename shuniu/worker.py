import contextlib
import signal
import sys
import time
import traceback
import multiprocessing
from typing import Dict

from shuniu.task import Task
from shuniu.signal_handle import timeout_handle, kill_handle, exit_handle, ExitError, UserKillError, UserTimeoutError

RUNNING = True


def task_timeout(timeout=3600):
    def decor(func):
        def stop_handle(signum, frame):
            global RUNNING
            RUNNING = False

        def run(*args, **kwargs):
            signal.signal(signal.SIGALRM, timeout_handle)
            signal.signal(signal.SIGCHLD, kill_handle)
            signal.signal(signal.SIGTERM, exit_handle)
            signal.signal(signal.SIGUSR1, stop_handle)
            signal.alarm(timeout)
            r = func(*args, **kwargs)
            signal.alarm(0)
            return r

        return run

    return decor


class LogSender:
    def __init__(self, log_queue: multiprocessing.Queue, worker_id: int):
        self.queue = log_queue
        self.wid = worker_id
        self.__logger__ = multiprocessing.get_logger()

    def __getattr__(self, item):
        def processor(*args, **kwargs):
            kwargs = {**kwargs, "extra": {"wid": self.wid}}
            try:
                self.queue.put((item, args, kwargs))
            except:
                self.__logger__.exception("log put error")

        return processor


class Worker(multiprocessing.Process):
    def __init__(self, registry, rpc, worker_id, task_queue, done_queue, log_queue):
        super().__init__()
        self.rpc = rpc
        self.worker_id = worker_id
        self.registry: Dict[int, Task] = registry
        self.task_queue = task_queue
        self.done_queue = done_queue
        self.log_queue = log_queue
        self.logger = LogSender(log_queue, self.worker_id)
        self.fork()

    def fork(self):
        fork_session = self.rpc.new_session()
        fork_session.cookies = self.rpc.__api__.cookies.copy()
        self.rpc.__api__ = fork_session

    def run(self) -> None:
        while RUNNING:
            try:
                task = self.task_queue.get()
                if not task:
                    break
                kwargs, task_id, src, task_type = task
                task_class = self.registry[task_type]
                if not task_class.forked:
                    task_class.__init_socket__()
                    task_class.forked = True
                task_class.mock(task_id=task_id, src=src, wid=self.worker_id)
                self.execute(task_class, kwargs["args"], kwargs["kwargs"])
            except ExitError:
                break
            except:
                self.logger.error(f"processing status failed: \n{traceback.format_exc()}")
            finally:
                self.done()
        self.logger.info("worker-{self.worker_id} exit")

    def execute(self, task: Task, args, kwargs):
        start_time = time.time()
        self.logger.info(f"Start {task.name}[{task.task_id}]")
        try:
            func = task_timeout(task.option.timeout)(task)
            result = func(args, kwargs)
            with contextlib.suppress(Exception):
                self.rpc.ack(task.task_id)
        except ExitError as e:
            self.logger.info(f"Task {task.name}[{task.task_id}] termination in {time.time() - start_time}")
            with contextlib.suppress(Exception):
                self.rpc.ack(task.task_id)
            raise e
        except UserKillError:
            exc_info = sys.exc_info()
            self.logger.info(f"Task {task.name}[{task.task_id}] termination in {time.time() - start_time}")
            with contextlib.suppress(Exception):
                self.rpc.ack(task.task_id)
            with contextlib.suppress(Exception):
                task.on_failure(*exc_info)
        except Exception as e:
            exc_info = sys.exc_info()
            error = "".join(traceback.format_exception(*exc_info))
            with contextlib.suppress(Exception):
                task.on_failure(*exc_info)
            if not isinstance(e, UserTimeoutError) and any(isinstance(e, ex) for ex in task.option.autoretry_for):
                with contextlib.suppress(Exception):
                    self.rpc.ack(task.task_id, retry=True)
                error = "recoverable exception\n" + error
            else:
                with contextlib.suppress(Exception):
                    self.rpc.ack(task.task_id, fail=True)
                error = "unrecoverable exception\n" + error
            self.logger.info(f"Task {task.name}[{task.task_id}] failure in {time.time() - start_time}\n" + error)
            if not task.option.ignore_result:
                with contextlib.suppress(Exception):
                    self.rpc.set(task.task_id, task.src, payload={"__traceback__": error}, **task.option)
        else:
            with contextlib.suppress(Exception):
                task.on_success()
            result = {} if isinstance(result, type(None)) else result
            if not task.option.ignore_result:
                with contextlib.suppress(Exception):
                    self.rpc.set(task.task_id, task.src, payload=result, **task.option)
            self.logger.info(f"Task {task.name}[{task.task_id}] succeeded in {time.time() - start_time}: {result}")

    def done(self):
        try:
            self.done_queue.put(self.worker_id)
        except:
            self.logger.exception("put worker_id error")
