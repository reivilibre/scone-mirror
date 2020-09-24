from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures.thread import ThreadPoolExecutor


class Pools:
    _instance = None

    def __init__(self):
        self.threaded = ThreadPoolExecutor()
        self.process = ProcessPoolExecutor()

    @staticmethod
    def get():
        if not Pools._instance:
            Pools._instance = Pools()
        return Pools._instance

    def shutdown(self):
        self.threaded.shutdown()
        self.process.shutdown()
