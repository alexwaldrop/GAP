import threading
import time
import math
import logging
import os

from System.Workers.Thread import Thread
from System.Datastore import GAPFile

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
        # Processor for executing task
        self.proc       = None
        # Workspace where task will be executed
        self.workspace  = None
        # Resources required to execute task
        self.cpus       = None
        self.mem        = None
        self.disk_space = None

        # Flag for whether TaskWorker was cancelled
        self.__cancelled = False

        # Output directory where final output will be saved
        self.final_output_dir = os.path.join(self.platform.get_final_output_dir(), self.task.get_ID())

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

            # Get module command to be run
            self.set_status(self.LOADING)
            self.cmd = self.__get_task_cmd()

            # Create processor capable of running job and run the module's command
            self.set_status(self.RUNNING)
            self.__run_task_cmd()

            # Save any output files produced by module
            self.set_status(self.FINALIZING)
            self.__save_task_output()

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
            # Destroy processor if it exists
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
            # Throw errors
            self.proc.lock()

    def is_success(self):
        return not self.__err

    def __get_task_cmd(self):
        # Generate and return module command that will be run to complete task

        # Define workspace where command will be run
        self.workspace = self.platform.get_workspace(self.task.get_ID())

        # Update module output directory to be consistent with workspace
        self.module.set_output_dir(self.workspace.get_wrk_output_dir())

        # Generate command to be executed
        return self.module.get_command()

    def __run_task_cmd(self):

        # Return if there's no command to be run
        if self.cmd is None:
            return

        task_id = self.task.get_ID()
        input_files = self.task.get_input_files()

        # Create and load processor capable of running task
        self.proc = self.platform.get_task_processor(task_id, self.cpus, self.mem, self.disk_space, input_files)

        # Run the command generated by the module
        self.proc.run_command(job_name=task_id, cmd=self.cmd)
        # Wait for command to finish and capture output
        out, err = self.proc.wait_process(task_id)

        # Post-process command output if necessary
        self.module.process_cmd_output(out, err)

    def __save_task_output(self):
        # Persist any output files produced by task
        # Final output types
        final_output_types = self.task.get_final_output_keys()

        # Get workspace places for output files
        final_output_dir = self.workspace.get_final_output_dir()
        tmp_output_dir = self.workspace.get_tmp_output_dir()

        for output_file in self.task.get_output_files():
            if output_file.get_type() in final_output_types:
                dest_dir = final_output_dir
            else:
                dest_dir = tmp_output_dir

            # Calculate output file size
            output_file.set_size(self.platform.calc_file_size(output_file))

            # Transfer to correct output directory
            self.platform.transfer(output_file, dest_dir)

            # Update path of output file to reflect new location
            output_file.update_path(dest_dir)

    def __clean_up(self):
        if self.proc is None:
            return

        # Try to return task log
        try:
            # Unlock processor if it's been locked so logs can be returned
            self.proc.unlock()
            self.__save_task_logs()
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

    def __save_task_logs(self):
        try:
            # Move log directory to final output log directory
            tmp_log_dir = self.workspace.get_wrk_log_dir()
            final_log_dir = self.workspace.get_final_log_dir()
            self.platform.transfer(tmp_log_dir, final_log_dir, log_transfer=False)

        except BaseException, e:
            # Handle but don't raise any exceptions and report why logs couldn't be returned
            logging.error("Unable to return logs for task '%s'!" % self.task.get_ID())
            if e.message != "":
                logging.error("Recieved the following error:\n%s" % e.message)

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


