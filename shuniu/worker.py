import sys
import time
import signal
import traceback
import multiprocessing

from shuniu.task import Task

RUNNING = True


class LogSender:
    def __init__(self, log_queue: multiprocessing.Queue, worker_id: int):
        self.queue = log_queue
        self.wid = worker_id

    def __getattr__(self, item):
        def processor(*args, **kwargs):
            kwargs = {**kwargs, "extra": {"wid": self.wid}}
            self.queue.put((item, args, kwargs))

        return processor


def handle_signal(*args):
    global RUNNING
    RUNNING = False


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGHUP, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


class Worker(multiprocessing.Process):
    def __init__(self, registry, rpc, worker_id, task_queue, done_queue, log_queue):
        super().__init__()
        self.rpc = rpc
        self.worker_id = worker_id
        self.registry = registry
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
            task = self.task_queue.get()
            kwargs, task_id, src, task_type = task
            task_class = self.registry[task_type]
            if not task_class.forked:
                task_class.__init_socket__()
                task_class.forked = True
            task_class.mock(task_id=task_id, src=src, wid=self.worker_id)
            try:
                self.execute(task_class, kwargs["args"], kwargs["kwargs"])
            except Exception as e:
                self.logger.error(f"processing status failed: {e}")
            finally:
                self.done()

    def execute(self, task: Task, args, kwargs):
        start_time = time.time()
        self.logger.info(f"Start {task.name}[{task.task_id}]")
        try:
            result = task(args, kwargs)
            self.rpc.ack(task.task_id)
        except Exception as e:
            exc_info = sys.exc_info()
            error = "".join(traceback.format_exception(*exc_info))
            if any(isinstance(e, ex) for ex in task.option.autoretry_for):
                self.rpc.ack(task.task_id, retry=True)
                self.logger.error("autoretry exception\n" + traceback.format_exc())
            else:
                self.rpc.ack(task.task_id, fail=True)
                self.logger.error("unknown exception\n" + traceback.format_exc())
            if not task.option.ignore_result:
                self.rpc.set(
                    task.task_id,
                    task.src,
                    payload={"__traceback__": error},
                    serialization=task.option.serialization,
                    compression=task.option.compression,
                )
            self.logger.info(f"Task {task.name}[{task.task_id}] failure in {time.time() - start_time}")
            task.on_failure(*exc_info)
        else:
            result = {} if isinstance(result, type(None)) else result
            if not task.option.ignore_result:
                self.rpc.set(
                    task.task_id,
                    task.src,
                    payload=result,
                    serialization=task.option.serialization,
                    compression=task.option.compression,
                )
            self.logger.info(f"Task {task.name}[{task.task_id}] succeeded in {time.time() - start_time}: {result}")
            task.on_success()

    def done(self):
        self.done_queue.put(self.worker_id)
