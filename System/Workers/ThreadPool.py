import threading
from Queue import Queue
import logging

# Inspired by https://www.metachris.com/2016/04/python-threadpool/
class PoolWorker(threading.Thread):
    """ Thread executing tasks from a given tasks queue """
    def __init__(self, task_queue, **kwargs):
        super(PoolWorker, self).__init__()
        self.task_queue = task_queue
        self.daemon = True
        self.start()

    def run(self):
        while True:
            try:
                args, kargs = self.task_queue.get()
                self.task(*args, **kargs)
            except Exception as e:
                # Raise abstract function for handling thread
                logging.error("%s failed!" % self.__class__.__name__)
                if e.message != "":
                    logging.error("Received the following error message:\n%s" % e.message)
            finally:
                # Mark this task as done, whether an exception happened or not
                self.task_queue.task_done()

    def task(self, *args, **kargs):
        pass

class ThreadPool:
    """ Pool of threads consuming tasks from a queue """
    def __init__(self, num_threads, worker_class=None, **worker_kwargs):
        # Create task queue
        self.tasks = Queue(num_threads)

        # Set class of Worker in thread pool
        self.worker_class = PoolWorker if worker_class is None else worker_class

        # Initialize thread pool workers
        self.workers = []
        for _ in range(num_threads):
            self.workers.append(self.worker_class(self.tasks, **worker_kwargs))

    def add_task(self, *args, **kargs):
        """ Add a task to the queue """
        self.tasks.put((args, kargs))

    def wait_completion(self):
        """ Wait for completion of all the tasks in the queue """
        self.tasks.join()