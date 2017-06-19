import threading
import Queue
import logging
import sys
import time

class Thread(threading.Thread):
    """ Exception throwing method taken from:
                   http://stackoverflow.com/questions/2829329/catch-a-threads-exception-in-the-caller-thread-in-python
    """

    def __init__(self):
        super(Thread, self).__init__()

        # Setting node thread as daemon
        self.daemon = True

        # Generating a queue for the exceptions that appear in the current thread
        self.exception_queue = Queue.Queue()

        # Setting a variable for error message that might appear
        self.err_msg = ""

        # Thread status
        self.finished = False

    def run(self):
        try:
            self.task()
        except BaseException as e:
            if e.message != "":
                logging.error("%s: %s." % (self.err_msg, e.message))
            else:
                logging.error("%s!" % self.err_msg)
            self.exception_queue.put(sys.exc_info())
        else:
            self.exception_queue.put(None)
        finally:
            self.finished = True

    def set_error_message(self, msg):
        self.err_msg = msg

    def task(self):
        raise NotImplemented("(%s) The thread method task() has not been implemented!" % self.name)

    def is_done(self):
        return self.finished

    def finalize(self):

        while not self.is_done():
            time.sleep(2)

        # If exception queue is empty at this point, then the thread has been finalized already
        if not self.exception_queue.empty():

            # Obtain the exception information from the Queue
            exc_info = self.exception_queue.get()

            # Raise the received exception
            if exc_info is not None:
                raise exc_info[0], exc_info[1], exc_info[2]