import os
import logging
import abc
from collections import OrderedDict
import subprocess as sp
import time
import threading

from System.Platform import Process, TaskPlatform

class TaskProcessor(object):
    __metaclass__ = abc.ABCMeta

    # Instance status values available between threads
    OFF         = 0  # Destroyed or not allocated on the cloud
    AVAILABLE   = 1  # Available for running processes
    BUSY        = 2  # Instance actions, such as create and destroy are running
    DEAD        = 3  # Instance is shutting down, as a DEAD signal was received
    MAX_STATUS  = 3  # Maximum status value possible

    def __init__(self, name, nr_cpus, mem, disk_space, **kwargs):
        self.name       = name
        self.nr_cpus    = nr_cpus
        self.mem        = mem
        self.disk_space = disk_space

        # Initialize the start and stop time
        self.start_time = None
        self.stop_time  = None

        # Get name of directory where logs will be written
        self.log_dir    = kwargs.pop("log_dir", None)

        # Get name of working directory
        self.wrk_dir    = kwargs.pop("wrk_dir", None)

        # Ordered dictionary of processing being run by processor
        self.processes  = OrderedDict()

        # Setting the instance status
        self.status_lock    = threading.Lock()
        self.status         = TaskProcessor.OFF

    def create(self):
        self.set_status(TaskProcessor.AVAILABLE)

    def destroy(self):
        self.set_status(TaskProcessor.OFF)

    def run(self, job_name, cmd, num_retries=0):

        # Checking if logging is required
        if "!LOG" in cmd:

            # Generate name of log file
            log_file = "%s.log" % job_name
            if self.log_dir is not None:
                log_file = os.path.join(self.log_dir, log_file)

            # Generating all the logging pipes
            log_cmd_null    = " >>/dev/null 2>&1 "
            log_cmd_stdout  = " >>%s " % log_file
            log_cmd_stderr  = " 2>>%s " % log_file
            log_cmd_all     = " >>%s 2>&1 " % log_file

            # Replacing the placeholders with the logging pipes
            cmd = cmd.replace("!LOG0!", log_cmd_null)
            cmd = cmd.replace("!LOG1!", log_cmd_stdout)
            cmd = cmd.replace("!LOG2!", log_cmd_stderr)
            cmd = cmd.replace("!LOG3!", log_cmd_all)

        # Save original command
        original_cmd = cmd

        # Make any modifications to the command to allow it to be run on a specific platform
        cmd = self.adapt_cmd(cmd)

        # Run command using subprocess popen and add Popen object to self.processes
        logging.info("(%s) Process '%s' started!" % (self.name, job_name))
        logging.debug("(%s) Process '%s' has the following command:\n    %s" % (self.name, job_name, original_cmd))

        # Generating process arguments
        kwargs = dict()

        # Process specific arguments
        kwargs["cmd"] = original_cmd

        # Popen specific arguments
        kwargs["shell"] = True
        kwargs["stdout"] = sp.PIPE
        kwargs["stderr"] = sp.PIPE
        kwargs["num_retries"] = num_retries

        # Add process to list of processes
        self.processes[job_name] = Process(cmd, **kwargs)

    def wait(self):
        # Returns when all currently running processes have completed
        for proc_name, proc_obj in self.processes.iteritems():
            self.wait_process(proc_name)

    def create_workspace(self, workspace):
        # Create all directories specified in task workspace
        logging.info("(%s) Creating workspace..." % self.name)

        # Create all workspace directories
        self.mkdir(workspace.get_wrk_dir())
        self.mkdir(workspace.get_log_dir())
        self.mkdir(workspace.get_tmp_output_dir())
        self.mkdir(workspace.get_final_output_dir())
        self.mkdir(workspace.get_final_log_dir())

        # Set processor wrk, log directories
        self.set_wrk_dir(workspace.get_wrk_dir())
        self.set_log_dir(workspace.get_log_dir())

        # Give everyone all the permissions on working directory
        logging.info("(%s) Updating workspace permissions..." % self.name)
        cmd = "sudo chmod -R 777 %s" % self.wrk_dir
        self.run(job_name="update_wrkspace_perms", cmd=cmd)

        # Wait for all the above commands to complete
        self.wait()
        logging.info("(%s) Successfully created workspace!" % self.name)

    def load_inputs(self, task_inputs):
        # Load inputs into workspace
        # Input can either be remote file, local file, or docker image
        seen = []
        for task_input in task_inputs:

            # Pull image if its docker image
            if task_input.is_docker_image():
                logging.debug("(%s) Updating")
                self.pull_docker_image(task_input.get_docker_image_name())

            # Transfer remote/local files to working directory
            else:
                self.transfer_file(src_path=task_input, dest_dir=self.wrk_dir, job_name="transfer_%s" % task_input.get_file_id())

        # Wait for all processes to finish
        self.wait()

    def pull_docker_image(self, docker_image_name):
        pass

    ############ Getters and Setters
    def set_status(self, new_status):
        # Updates instance status with threading.lock() to prevent race conditions
        if new_status > TaskProcessor.MAX_STATUS or new_status < 0:
            logging.debug("(%s) Status level %d not available!" % (self.name, new_status))
            raise RuntimeError("Instance %s has failed!" % self.name)
        with self.status_lock:
            self.status = new_status

    def get_status(self):
        # Returns instance status with threading.lock() to prevent race conditions
        with self.status_lock:
            return self.status

    def set_log_dir(self, new_log_dir):
        self.log_dir = new_log_dir

    def set_wrk_dir(self, new_wrk_dir):
        self.wrk_dir = new_wrk_dir

    def set_start_time(self):
        if self.start_time is None:
            self.start_time = time.time()

    def set_stop_time(self):
        self.stop_time = time.time()

    def get_name(self):
        return self.name

    def get_runtime(self):
        if self.start_time is None:
            return 0
        elif self.stop_time is None:
            return time.time() - self.start_time
        else:
            return self.stop_time - self.start_time

    def get_nr_cpus(self):
        return self.nr_cpus

    def get_mem(self):
        return self.mem

    def get_disk_space(self):
        return self.disk_space

    def compute_cost(self):
        # Compute running cost of current task processor
        return 0

    ############ Abstract methods
    @abc.abstractmethod
    def wait_process(self, proc_name):
        pass

    @abc.abstractmethod
    def adapt_cmd(self, cmd):
        pass

    @abc.abstractmethod
    def path_exists(self, path):
        pass

    @abc.abstractmethod
    def mkdir(self, dir_path):
        pass

    @abc.abstractmethod
    def transfer_file(self, src_path, dest_dir, dest_file=None, log_transfer=True):
        # Transfer a remote file from src_path to a local directory dest_dir
        # Log the transfer unless otherwise specified

        # Create job name
        job_name = "transfer_%s_%s" % (src_path.get_file_id(), TaskPlatform.generate_unique_id())

        # Transfer file from remote storage to task processor local storage
        if src_path.is_remote() and not dest_dir.is_remote():
            pass

        # Transfer file from task processor local storage to remote storage
        elif dest_dir.is_remote() and not src_path.is_remote():
            pass

        # Transfer file between two locations on task processor local storage
        elif not dest_dir.is_remote() and not dest_dir.is_remote():
            pass


