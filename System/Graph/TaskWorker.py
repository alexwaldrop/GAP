import threading
import time
import math
import logging

from System.Workers.Thread import Thread
from ModuleExecutor import ModuleExecutor

class TaskWorker(Thread):

    IDLE            = 0
    LOADING         = 1
    RUNNING         = 2
    FINALIZING      = 3
    COMPLETE        = 4
    CANCELLING      = 5
    FINALIZED       = 6

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

        # Processor for executing task
        self.proc       = None

        # Module command executor
        self.module_executor = None

        # Flag for whether task successfully completed
        self.__err = True

        # Flag for whether TaskWorker was cancelled
        self.__cancelled = False

        # Command that was run to carry out task
        self.cmd = None

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

    def get_runtime(self):
        if self.proc is None:
            return 0
        else:
            return self.proc.get_runtime()

    def get_cost(self):
        if self.proc is None:
            return 0
        else:
            return self.proc.compute_cost()

    def get_start_time(self):
        if self.proc is None:
            return None
        else:
            return self.proc.get_start_time()

    def get_cmd(self):
        return self.cmd

    def work(self):
        # Run task module command and save outputs
        try:
            # Set the input arguments that will be passed to the task module
            self.datastore.set_task_input_args(self.task.get_ID())

            # Compute task resource requirements
            cpus    = self.module.get_argument("nr_cpus")
            mem     = self.module.get_argument("mem")

            # Compute disk space requirements
            docker_image    = None
            input_files     = self.datastore.get_task_input_files(self.task.get_ID())
            if self.task.get_docker_image_id() is not None:
                docker_image    = self.datastore.get_docker_image(docker_id=self.task.get_docker_image_id())
            disk_space      = self.__compute_disk_requirements(input_files, docker_image)
            logging.debug("(%s) CPU: %s, Mem: %s, Disk space: %s" % (self.task.get_ID(), cpus, mem, disk_space))

            # Wait for platform to have enough resources to run task
            while not self.platform.can_make_processor(cpus, mem, disk_space) and not self.is_cancelled():
                time.sleep(5)

            # Quit if pipeline is cancelled
            self.__check_cancelled()

            # Define unique workspace for task input/output
            task_workspace = self.datastore.get_task_workspace(task_id=self.task.get_ID())
            logging.debug("(%s) Task workspace:\n%s" % (self.task.get_ID(), task_workspace.debug_string()))

            # Specify that module output files should be placed in task's working directory
            self.module.set_output_dir(task_workspace.get_wrk_dir())

            # Run command on processor if there's one to run
            if self.module.get_command() is not None:

                # Execute command if one exists
                self.set_status(self.LOADING)

                # Get processor capable of running job
                self.proc = self.platform.get_processor(self.task.get_ID(), cpus, mem, disk_space)

                self.module_executor = ModuleExecutor(task_id=self.task.get_ID(),
                                                      processor=self.proc,
                                                      workspace=task_workspace,
                                                      docker_image=docker_image)

                # Load task inputs onto module executor
                self.module_executor.load_input(input_files)

                # Update module's command to reflect changes to input paths
                self.set_status(self.RUNNING)
                self.cmd = self.module.update_command()
                out, err = self.module_executor.run(self.cmd)

                # Check to see if pipeline has been cancelled
                self.__check_cancelled()

                # Post-process command output if necessary
                self.module.process_cmd_output(out, err)

                # Save output files in workspace output dirs (if any)
                self.set_status(self.FINALIZING)
                output_files        = self.datastore.get_task_output_files(self.task.get_ID())
                final_output_types  = self.task.get_final_output_keys()
                if len(output_files) > 0:
                    self.module_executor.save_output(output_files, final_output_types)

            # Indicate that task finished without any errors
            if not self.__cancelled:
                with self.status_lock:
                    self.__err = False

        except BaseException, e:
            # Handle but do not raise exception if job was externally cancelled
            if self.__cancelled:
                logging.warning("Task '%s' failed due to cancellation!" % self.task.get_ID())

            else:
                # Raise exception if job failed for any reason other than cancellation
                self.set_status(self.FINALIZING)
                logging.error("Task '%s' failed!" % self.task.get_ID())
                raise
        finally:
            # Return logs and destroy processor if they exist
            logging.debug("TaskWorker '%s' cleaning up..." % self.task.get_ID())
            self.__clean_up()
            # Notify that task worker has completed regardless of success
            self.set_status(TaskWorker.COMPLETE)

    def cancel(self):
        # Cancel pipeline during runtime

        # Don't do anything if task has already finished or is finishing
        if self.get_status() in [self.FINALIZING, self.COMPLETE, self.CANCELLING, self.FINALIZED]:
            return

        # Set pipeline to cancelling and stop any currently running jobs
        logging.error("Task '%s' cancelled!" % self.task.get_ID())
        self.set_status(self.CANCELLING)
        self.__cancelled = True

        if self.proc is not None:
            # Prevent further commands from being run on processor
            self.proc.stop()

    def is_success(self):
        return not self.__err

    def is_cancelled(self):
        with self.status_lock:
            return self.__cancelled

    def __clean_up(self):

        # Do nothing if errors occurred before processor was even created
        if self.proc is None:
            return

        # Try to return task log
        try:
            # Unlock processor if it's been locked so logs can be returned
            if self.module_executor is not None and not self.__cancelled:
                self.module_executor.save_logs()
        except BaseException, e:
            logging.error("Unable to return logs for task '%s'!" % self.task.get_ID())
            if e.message != "":
                logging.error("Received following error:\n%s" % e.message)

        # Try to destroy platform if it's not off
        try:
            # Destroy processor if it hasn't already been destroyed
            if not "destroy" in self.proc.processes:
                self.proc.destroy(wait=False)
            # Wait until processor is destroyed
            self.proc.wait_process("destroy")
        except BaseException, e:
            logging.error("Unable to destroy processor '%s' for task '%s'" % (self.proc.get_name(), self.task.get_ID()))
            if e.message != "":
                logging.error("Received following error:\n%s" % e.message)

    def __compute_disk_requirements(self, input_files, docker_image, input_multiplier=2):
        # Compute size of disk needed to store input/output files
        input_size = 0

        # Add size of docker image if one needs to be loaded for task
        if docker_image is not None:
            input_size += docker_image.get_size()

        # Add sizes of each input file
        for input_file in input_files:
            input_size += input_file.get_size()

        # Set size of desired disk
        disk_size = int(math.ceil(input_multiplier * input_size))

        # Make sure platform can create a disk that size
        min_disk_size = self.platform.get_min_disk_space()
        max_disk_size = self.platform.get_max_disk_space()

        # Must be at least as big as minimum disk size
        disk_size = max(disk_size, min_disk_size)

        # And smaller than max disk size
        disk_size = min(disk_size, max_disk_size)
        return disk_size

    def __check_cancelled(self):
        if self.__cancelled:
            raise RuntimeError("(%s) Task failed due to cancellation!")
