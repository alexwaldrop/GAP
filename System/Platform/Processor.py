import os
import logging
import abc
from collections import OrderedDict
import subprocess as sp
import time
import threading

from System.Platform import Process

class Processor(object):
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
        self.status         = Processor.OFF

        # Lock for preventing further commands from being run on processor
        self.locked = False

    def create(self):
        self.set_status(Processor.AVAILABLE)

    def destroy(self):
        self.set_status(Processor.OFF)

    def run(self, job_name, cmd, num_retries=0, docker_image=None):

        # Throw error if attempting to run command on stopped processor
        if self.locked:
            logging.error("(%s) Attempt to run process '%s' on stopped processor!" % (self.name, job_name))
            raise RuntimeError("Attempt to run process of stopped processor!")

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

        # Run in docker image if specified
        if docker_image is not None:
            cmd = "docker run -it -v %s:%s %s %s" % (self.wrk_dir, self.wrk_dir, docker_image, cmd)

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
        kwargs["docker_image"] = docker_image

        # Add process to list of processes
        self.processes[job_name] = Process(cmd, **kwargs)

    def wait(self):
        # Returns when all currently running processes have completed
        for proc_name, proc_obj in self.processes.iteritems():
            self.wait_process(proc_name)

    def lock(self):
        # Prevent any additional processes from being run
        with threading.Lock():
            self.locked = True

    def unlock(self):
        # Allow processes to run on processor
        with threading.Lock():
            self.locked = False

    def stop(self):
        # Lock so that no new processes can be run on processor
        self.lock()

        # Kill all currently executing processes on processor
        for proc_name, proc_obj in self.processes.iteritems():
            if not proc_obj.is_complete() and proc_name.lower() != "destroy":
                logging.debug("(%s) Killing process: %s" % (self.name, proc_name))
                proc_obj.terminate()

    ############ Getters and Setters
    def set_status(self, new_status):
        # Updates instance status with threading.lock() to prevent race conditions
        if new_status > Processor.MAX_STATUS or new_status < 0:
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
