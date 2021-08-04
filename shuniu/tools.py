import logging


class Singleton(object):
    def __init__(self, cls):
        self._cls = cls
        self._instance = {}

    def __call__(self, *args, **kwargs):
        if self._cls not in self._instance:
            self._instance[self._cls] = self._cls(*args, **kwargs)
        return self._instance[self._cls]


class WorkerLogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, "wid"):
            record.wid = "Main"
        return True
