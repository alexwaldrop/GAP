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

        # Lock for preventing further commands from being run on processor
        self.locked = False

    def create(self):
        self.set_status(TaskProcessor.AVAILABLE)

    def destroy(self):
        self.set_status(TaskProcessor.OFF)

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
            cmd = "docker -image %s -v%s/%s -runit %s" % (docker_image, self.wrk_dir, self.wrk_dir, cmd)

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

    def create_workspace(self, workspace):
        # Create all directories specified in task workspace

        logging.info("(%s) Creating workspace..." % self.name)
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
        # Inputs: list containing remote files, local files, and docker images
        seen = []
        for task_input in task_inputs:

            # Pull docker image if it hasn't already been pulled
            if task_input.is_docker_image() and task_input.get_docker_image_name() not in seen:
                docker_image = task_input.get_docker_image_name()
                logging.debug("(%s) Pulling docker image '%s'..." % (self.name, docker_image))

                # Add docker image name to list of active docker images in workspace
                seen.append(task_input.get_docker_image_name())

                # Pull docker image from remote repo
                self.pull_docker_image(task_input.get_docker_image_name())

            # Transfer remote/local files to working directory if they (or containing directory) haven't already been transferred
            elif task_input.is_remote() and task_input.get_transferrable_path() not in seen:
                logging.debug("(%s) Downloading remote file '%s'..." % (self.name, task_input.get_path()))

                # Add transfer path to list of remote paths that have been transferred to local workspace
                seen.append(task_input.get_transferrable_path())

                # Download remote file to local workspace
                self.mv(src_path=task_input, dest_dir=self.wrk_dir)
                logging.debug("(%s) Updated path: %s" % (self.name, task_input.get_path()))

            # Update paths of remote files that whose containing directories were already transferred
            # No need to actually transfer these files as they already exist locally
            elif task_input.is_remote() and task_input.get_transferrable_path() in seen:

                # Update the path to reflect transfer
                task_input.update_path(new_dir=self.wrk_dir)

        # Recursively give every permission to all files we just added
        logging.info("(%s) Updating workspace permissions..." % self.name)
        cmd = "sudo chmod -R 777 %s" % self.wrk_dir
        self.run(job_name="final_wrkspace_perm_update", cmd=cmd)

        # Wait for all processes to finish
        self.wait()

    def mv(self, src_path, dest_dir, log_transfer=True, wait=False, num_retries=1):
        # Transfer a remote file from src_path to a local directory dest_dir
        # Log the transfer unless otherwise specified
        # Wait=False: Return immediately. Wait=True: Return after transfer has completed.

        # Create job name
        job_name = "transfer_%s_%s" % (src_path.get_file_id(), TaskPlatform.generate_unique_id())

        # Get path that actually needs to be transferred (include wildcards, containing directory)
        path_to_transfer = src_path.get_transferable_path()

        # Move file between remote and local storage
        if src_path.is_remote() or dest_dir.is_remote():
            self.remote_mv(path_to_transfer, dest_dir, job_name, log_transfer, num_retries)

        # Transfer local storage file to new location on local storage
        elif not dest_dir.is_remote() and not dest_dir.is_remote():
            # This is just a normal move cmd
            self.__mv(path_to_transfer, dest_dir, job_name, log_transfer, num_retries)

        # Update file path to reflect new location
        src_path.update_path(new_dir=dest_dir)

        # Optionally wait for job to finish
        if wait:
            self.wait_process(job_name)

    def mkdir(self, dir_path, wait=False, num_retries=1):
        # Makes a directory if it doesn't already exists
        # Create job name
        job_name = "mkdir_%s_%s" % (dir_path.get_file_id(), TaskPlatform.generate_unique_id())

        if dir_path.is_remote():
            # Get command for checking if remote file exists
            self.remote_mkdir(dir_path, job_name, num_retries)
        else:
            # Get command for checking if local file exists
            self.__mkdir(dir_path, job_name, num_retries)

        # Optionally wait for command to finish
        if wait:
            self.wait_process(job_name)

    def path_exists(self, path, num_retries=1):
        # Determine if a path exists either locally on platform or remotely
        # Create job name
        job_name = "checkExists_%s_%s" % (path.get_file_id(), TaskPlatform.generate_unique_id())

        # Check remote file existence
        if path.is_remote():
            return self.remote_path_exists(path, job_name, num_retries)
        # Check local file existence
        else:
            return self.__path_exists(path, job_name, num_retries)

    def get_file_size(self, path, num_retries=1):
        # Determine file size
        # Create job name
        job_name = "getSize_%s_%s" % (path.get_file_id(), TaskPlatform.generate_unique_id())

        # Get remote file size
        if path.is_remote():
            return self.get_remote_file_size(path, job_name, num_retries)

        # Get local file size
        else:
            return self.__get_file_size(path, job_name, num_retries)

    def pull_docker_image(self, docker, wait=False, num_retries=1):
        # Pull docker image and make available on platform

        image_name = docker.get_image_name()
        image_tag = docker.get_tag()

        job_name = "docker_pull_%s" % image_name
        cmd = "docker pull %s" % image_name if image_tag is None else "docker pull %s:%s" % (image_name, image_tag)

        self.run(job_name, cmd, num_retries=num_retries)

        # Wait if necessary
        if wait:
            self.wait_process(job_name)

    def docker_image_exists(self, docker, num_retries=1):
        pass

    def get_docker_size(self, path, num_retries=1):
        pass

    def __mv(self, src_path, dest_dir, job_name, log_transfer=True, num_retries=1):
        # Move file on local storage
        cmd = "mv %s %s" % (src_path, dest_dir)
        if log_transfer:
            cmd = "%s !LOG3!" % cmd
        self.run(job_name, cmd, num_retries=num_retries)

    def __mkdir(self, path, job_name, num_retries=1):
        # Make directory on local storage
        cmd = "mkdir -p %s" % path
        self.run(job_name, cmd, num_retries=num_retries)

    def __path_exists(self, path, job_name, num_retries=1):
        # Check if path exists on local storage
        cmd = "ls %s" % path
        self.run(job_name, cmd, num_retries=num_retries)
        try:
            out, err = self.wait_process(job_name)
            return len(err) == 0
        except RuntimeError:
            return False
        except:
            logging.error("(%s) Unable to check path existence: %s" % (self.name, path))
            raise

    def __get_file_size(self, path, num_retries=1):
        # Get size of local file
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

    def remote_mv(self, src_path, dest_dir, job_name, log_transfer=True, num_retries=1):
        pass

    def remote_mkdir(self, path, job_name, num_retries=1):
        pass

    def remote_path_exists(self, path, job_name, num_retries=1):
        pass

    def get_remote_file_size(self, path, job_name, num_retries=1):
        pass
