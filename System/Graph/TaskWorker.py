import threading
import time
import math

from System.Workers.Thread import Thread
from System.Datastore import GAPFile

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
        self.module = self.task.get_module()

        # Datastore for getting/setting task output
        self.datastore = datastore

        # Platform upon which task will be executed
        self.platform = platform

        # Status attributes
        self.status_lock = threading.Lock()
        self.status = TaskWorker.INITIALIZING

        # Resources required to run task
        self.cpus       = None
        self.mem        = None
        self.disk_space = None

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
        self.cpus       = self.module.get_arguments("nr_cpus")
        self.mem        = self.module.get_arguments("mem")
        self.disk_space = self.__compute_disk_requirements()

        # Tell Scheduler this task is ready to run when its resource requirements are met
        self.set_status(TaskWorker.READY)

        while self.get_status() not in [TaskWorker.RUNNING, TaskWorker.CANCELLED]:
            # Wait for Scheduler to free up resources or cancel the job
            time.sleep(2)

        # Execute task
        if TaskWorker.RUNNING:

            # Run the module command if necessary (null modules produce 'None' as command)
            if self.module.get_command() is not None:
                self.platform.run_module(self.module)

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

    def get_cpus(self):
        return self.cpus

    def get_mem(self):
        return self.mem

    def get_disk_space(self):
        return self.disk_space

    def __set_input_args(self):

        # Get required arguments for task module
        task_id = self.task.get_ID()
        input_types = self.task.get_input_keys()

        # Get and set arg values from datastore
        for input_type in input_types:
            val = self.datastore.get_task_arg(task_id, input_type)
            if val is not None:
                self.task.get_module().set_argument(input_type, val)

        # Make sure nr_cpus, mem arguments are properly formatted
        self.__format_nr_cpus()
        self.__format_mem()

    def __format_nr_cpus(self):
        # Makes sure the argument for nr_cpus is valid
        nr_cpus  = self.module.get_arguments("nr_cpus")
        max_cpus = self.platform.get_max_cpus()

        # CPUs = 'max' converted to platform maximum cpus
        if isinstance(nr_cpus, basestring) and nr_cpus.lower() == "max":
            # Set special case for maximum nr_cpus
            nr_cpus = max_cpus

        # CPUs > 'max' converted to maximum cpus
        elif nr_cpus > max_cpus:
            nr_cpus = max_cpus

        # Update module nr_cpus argument
        self.module.set_argument("nr_cpus", int(nr_cpus))

    def __format_mem(self):
        mem = self.module.get_argments("mem")
        nr_cpus = self.module.get_arguments("nr_cpus")
        max_mem = self.platform.get_max_mem()
        if isinstance(mem, basestring):
            # Special case where mem is platform max
            if mem.lower() == "max":
                mem = max_mem
            # Special case if memory is scales with nr_cpus (e.g. 'nr_cpus * 1.5')
            elif "nr_cpus" in mem.lower():
                mem_expr = mem.lower()
                mem = int(eval(mem_expr.replace("nr_cpus", str(nr_cpus))))
        # Set to platform max mem if over limit
        if mem > max_mem:
            mem = max_mem
        # Update module memory argument
        self.module.set_argument("mem", int(mem))

    def __compute_disk_requirements(self, input_multiplier=2):
        # Compute size of disk needed to store input/output files
        input_size = 0
        args = self.task.get_input_args()
        for arg_key, arg in args.iteritems():
            input = arg.get_value()
            # Determine if arg is a file
            if isinstance(input.get_value(), GAPFile):
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


