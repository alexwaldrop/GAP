import logging
import threading
import time
import math

from System.Workers.Thread import Thread

class TaskWorker(Thread):

    INITIALIZING    = 0
    READY           = 1
    RUNNING         = 2
    COMPLETE        = 3
    CANCELLED       = 4

    def __init__(self, task, datastore, platform):
        # Class for executing task

        # Initialize new thread
        err_msg = "TaskWorker for %s has stopped working!" % task.get_ID()
        super(TaskWorker, self).__init__(err_msg)

        # Task to be executed
        self.task = task

        # Datastore for getting/setting task output
        self.datastore = datastore

        # Platform upon which task will be executed
        self.platform = platform

        # Status attributes
        self.status_lock = threading.Lock()
        self.status = TaskWorker.INITIALIZING

    def task(self):
        try:
            # Try to execute task
            self.execute()
        finally:
            # Notify that task worker has completed regardless of success
            self.set_status(TaskWorker.COMPLETE)

    def execute(self):

        # Set the input arguments that will be passed to the task module
        self.__set_input_args()

        # Compute and set task resource requirements
        args = self.task.get_input_args()
        self.task.set_cpus(args["nr_cpus"].get_value())
        self.task.set_mem(args["mem"].get_value())
        self.task.disk_space(self.__compute_disk_requirements())

        # Tell Scheduler this task is ready to run when its resource requirements are met
        self.set_status(TaskWorker.READY)

        while self.get_status() not in [TaskWorker.RUNNING, TaskWorker.CANCELLED]:
            # Wait for Scheduler to free up resources or cancel the job
            time.sleep(2)

        # Execute task
        if TaskWorker.RUNNING:

            # Get the module command
            cmd = self.task.get_command(self.platform)

            # Run the module command if available
            if cmd is not None:
                self.platform.run_task(self.task)

    def set_status(self, new_status):
        # Updates instance status with threading.lock() to prevent race conditions
        with self.status_lock:
            self.status = new_status

    def get_status(self):
        # Returns instance status with threading.lock() to prevent race conditions
        with self.status_lock:
            return self.status

    def get_task(self):
        return self.task

    def __set_input_args(self):

        # Get required arguments for task module
        task_id = self.task.get_ID()
        arguments = self.task.get_module().get_arguments()

        # Get and set arg values from datastore
        for arg_key, arg in arguments.iteritems():
            val = self.datastore.get_task_arg(task_id, type=arg_key)
            arg.set(val)

    def __compute_disk_requirements(self, input_multiplier=2):
        # Compute size of disk needed to store input/output files
        input_size = 0
        args = self.task.get_input_args()
        for arg_key, arg in args.iteritems():
            input = arg.get_value()
            # Determine if arg is a file
            if isinstance(input.get_value(), "GAPFile"):
                # Check to see if file size exists
                if input.get_file_size() is None:
                    # Calc input size of file
                    input.set_file_size(self.platform.calc_file_size(input))
                # Increment input file size (GB)
                input_size += input.get_file_size()

        # Set size of desired disk
        disk_size = int(math.ceil(input_multiplier * input_size))

        # Make sure platform can create a disk that size
        min_disk_size = self.platform.get_min_disk_size()
        max_disk_size = self.platform.get_max_disk_size()

        # Must be at least as big as minimum disk size
        disk_size = max(disk_size, min_disk_size)

        # And smaller than max disk size
        disk_size = min(disk_size, max_disk_size)
        #logging.debug("Computing disk size for task '%s': %s" % (self.task.get_ID(), disk_size))
        return disk_size


