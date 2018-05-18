import threading
import time
import math
import logging

from System.Workers.Thread import Thread
from System.Datastore import GAPFile
from ModuleExecutor import ModuleExecutor

class TaskWorker(Thread):

    IDLE            = 0
    LOADING         = 1
    RUNNING         = 2
    FINALIZING      = 3
    COMPLETE        = 4
    CANCELLING      = 5

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
        self.status = TaskWorker.IDLE

        # Command to be executed to complete task
        self.cmd        = None

        # Resources required to execute task
        self.cpus       = None
        self.mem        = None
        self.disk_space = None

        # Processor for executing task
        self.proc       = None

        # Module command executor
        self.module_executor = None

        # Flag for whether task successfully completed
        self.__err = True

        # Flag for whether TaskWorker was cancelled
        self.__cancelled = False

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

    def task(self):
        # Run task module command and save outputs
        try:
            # Set the input arguments that will be passed to the task module
            self.__set_task_module_input()

            # Compute task resource requirements
            self.cpus        = self.module.get_arguments("nr_cpus")
            self.mem         = self.module.get_arguments("mem")
            self.disk_space  = self.__compute_disk_requirements()

            # Wait for platform to have enough resources to run task
            while not self.platform.can_run_task(self.cpus, self.mem, self.disk_space) and not self.get_status() is self.CANCELLING:
                time.sleep(5)

            # Define unique workspace for task input/output
            task_workspace = self.datastore.get_task_workspace(task_id=self.task.get_ID())

            # Specify that module output files should be placed in task workspace output dir
            self.module.set_output_dir(task_workspace.get_wrk_output_dir())

            # Create processor capable of running job
            self.proc = self.platform.get_processor(self.task.get_ID(), self.cpus, self.mem, self.disk_space)

            # Get module command to be run
            task_cmd = self.module.get_command()

            if task_cmd is not None:

                # Execute command if one exists
                self.set_status(self.LOADING)
                self.module_executor = ModuleExecutor(task_id=self.task.get_ID(),
                                                      processor=self.proc,
                                                      workspace=task_workspace)

                # Load task inputs onto module executor
                self.module_executor.load_input(self.task.get_inputs())

                # Run module's command
                self.set_status(self.RUNNING)
                out, err = self.module_executor.run(task_cmd)

                # Post-process command output if necessary
                self.module.process_cmd_output(out, err)


            # Save output files in workspace output dirs (if any)
            self.set_status(self.FINALIZING)
            output_files        = self.task.get_output_files()
            final_output_types  = self.task.get_final_output_keys()
            if len(output_files) > 0:
                self.module_executor.save_output(output_files, final_output_types)

            # Indicate that task finished without any errors
            with self.status_lock:
                self.__err = False

        except BaseException, e:
            # Handle but do not raise exception if job was externally cancelled
            if self.__cancelled:
                logging.warning("Task with id '%s' failed due to cancellation!")

            else:
                # Raise exception if job failed for any reason other than cancellation
                raise
        finally:
            # Return logs and destroy processor if they exist
            self.__clean_up()
            # Notify that task worker has completed regardless of success
            self.set_status(TaskWorker.COMPLETE)

    def cancel(self):
        # Cancel pipeline during runtime

        # Don't do anything if task has already finished or is finishing
        if self.get_status() in [self.FINALIZING, self.COMPLETE, self.CANCELLING]:
            return

        # Set pipeline to cancelling and stop any currently running jobs
        logging.error("Task '%s' cancelled!" % self.task.get_ID())
        self.set_status(self.CANCELLING)
        self.__cancelled = True

        if self.proc is not None:
            # Stop any processes currently running on processor
            self.proc.stop()

    def is_success(self):
        return not self.__err

    def __clean_up(self):

        # Do nothing if errors occurred before processor was even created
        if self.proc is None:
            return

        # Unlock processor in case task was cancelled and processor was locked
        self.proc.unlock()

        # Try to return task log
        try:
            # Unlock processor if it's been locked so logs can be returned
            if self.module_executor is not None:
                self.module_executor.save_logs()
        except BaseException, e:
            logging.error("Unable to return logs for task '%s'!" % self.task.get_ID())
            if e.message != "":
                logging.error("Received following error:\n%s" % e.message)

        # Try to destroy platform
        try:
            self.proc.destroy()
        except BaseException, e:
            logging.error("Unable to destroy processor '%s' for task '%s'" % (self.proc.get_name(), self.task.get_ID()))
            if e.message != "":
                logging.error("Received following error:\n%s" % e.message)

    def __set_task_module_input(self):

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
        input_files = self.task.get_input_files()
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


