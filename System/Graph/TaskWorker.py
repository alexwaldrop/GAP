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
            return self.proc.get_cost()

    def task(self):
        # Run task module command and save outputs
        try:
            # Set the input arguments that will be passed to the task module
            self.datastore.set_task_input_args(self.task.get_ID())

            # Compute task resource requirements
            cpus    = self.module.get_arguments("nr_cpus")
            mem     = self.module.get_arguments("mem")

            # Compute disk space requirements
            input_files     = self.datastore.get_task_input_files(self.task.get_ID())
            docker_image    = self.datastore.get_docker_image(docker_id=self.task.get_docker_image_id())
            disk_space      = self.__compute_disk_requirements(input_files, docker_image)

            # Wait for platform to have enough resources to run task
            while not self.platform.can_run_task(cpus, mem, disk_space) and not self.get_status() is self.CANCELLING:
                time.sleep(5)

            # Define unique workspace for task input/output
            task_workspace = self.datastore.get_task_workspace(task_id=self.task.get_ID())

            # Specify that module output files should be placed in task workspace output dir
            self.module.set_output_dir(task_workspace.get_wrk_output_dir())

            # Get module command to be run
            task_cmd = self.module.get_command()

            if task_cmd is not None:

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

                # Run module's command
                self.set_status(self.RUNNING)
                out, err = self.module_executor.run(task_cmd)

                # Post-process command output if necessary
                self.module.process_cmd_output(out, err)


            # Save output files in workspace output dirs (if any)
            self.set_status(self.FINALIZING)
            output_files        = self.datastore.get_task_output_files(self.task.get_ID())
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

    def __compute_disk_requirements(self, input_files, docker_image, input_multiplier=2):
        # Compute size of disk needed to store input/output files
        input_size = 0

        # Add size of docker image if one needs to be loaded for task
        if docker_image is not None:
            input_size += docker_image.get_size()

        # Add sizes of each input file
        for input_file in input_files:
            input_size += input_file.get_file_size()

        # Set size of desired disk
        disk_size = int(math.ceil(input_multiplier * input_size))

        # Make sure platform can create a disk that size
        min_disk_size = self.platform.get_min_disk_size()
        max_disk_size = self.platform.get_max_disk_size()

        # Must be at least as big as minimum disk size
        disk_size = max(disk_size, min_disk_size)

        # And smaller than max disk size
        disk_size = min(disk_size, max_disk_size)
        return disk_size


