import logging
import os
import abc
import uuid
import threading

from Config import ConfigParser
from BaseProcessor import BaseProcessor

class BasePlatform(object):
    __metaclass__ = abc.ABCMeta

    CONFIG_SPEC = None

    def __init__(self, name, platform_config_file, final_output_dir):

        # Platform name
        self.name = name

        # Initialize platform config
        config_parser       = ConfigParser(platform_config_file, self.CONFIG_SPEC)
        self.config         = config_parser.get_config()

        # Platform-wide resource limits
        self.TOTAL_NR_CPUS      = self.config["PLAT_MAX_NR_CPUS"]
        self.TOTAL_MEM          = self.config["PLAT_MAX_MEM"]
        self.TOTAL_DISK_SPACE   = self.config["PLAT_MAX_DISK_SPACE"]

        # Single process resource limit
        self.MAX_NR_CPUS        = self.config["PROC_MAX_NR_CPUS"]
        self.MAX_MEM            = self.config["PROC_MAX_MEM"]
        self.MAX_DISK_SPACE     = self.config["PROC_MAX_DISK_SPACE"]

        # Define workspace directory names
        self.wrk_dir            = self.config["workspace_dir"]
        self.final_output_dir   = self.standardize_dir(final_output_dir)

        # Dictionary to hold processors currently managed by the platform
        self.processors = {}

        # Platform resource threading lock
        self.platform_lock = threading.Lock()

        # Boolean flag to lock processor creation upon cleanup
        self.__locked = False

    def get_curr_usage(self):
        # Return total cpus, mem, disk space currently in use on platform
        with self.platform_lock:
            cpu = 0
            mem = 0
            disk_space = 0
            for processor_id, processor in self.processors.iteritems():
                if processor.get_status() > BaseProcessor.OFF:
                    cpu += processor.get_cpu()
                    mem += processor.get_mem()
                    disk_space += processor.get_disk_space()
        return cpu, mem, disk_space

    def can_run_task(self, task_nr_cpus, task_mem, task_disk_space):
        cpu, mem, disk_space = self.get_curr_usage()
        cpu_overload    = cpu + task_nr_cpus > self.MAX_NR_CPUS
        mem_overload    = mem + task_mem > self.MAX_MEM
        disk_overload   = disk_space + task_disk_space > self.MAX_DISK_SPACE
        return (not cpu_overload) and (not mem_overload) and (not disk_overload) and (not self.__locked)

    def get_task_processor(self, task_id, nr_cpus, mem, disk_space):
        # Ensure unique name for processor
        name        = "proc-%s-%s-%s" % (self.name[:20], task_id[:25], self.generate_unique_id())
        logging.info("Creating processor '%s' for task '%s'..." % (name, task_id))

        # Initialize new processor with enough CPU/mem/disk space to complete task
        processor   = self.init_task_processor(name, nr_cpus, mem, disk_space)

        # Add to list of processors if not already there
        if task_id not in self.processors:
            self.processors[task_id] = processor
        else:
            logging.error("Platform cannot create task processor with duplicate id: '%s'!" % task_id)
            raise RuntimeError("Platform attempted to create duplicate task processor!")

        logging.info("Processor '%s' (%d vCPUs, %dGB RAM, %dGB disk space) ready for processing!" % (name, processor.nr_cpus, processor.mem, processor.disk_space))
        return self.processors[task_id]

    def load_task_processor(self, task_id, workspace, input_files):
        # Create task workspace on assigned processor. Load necessary input files

        # Launch task processor that has already been initialized
        proc = self.processors[task_id]
        proc_name = proc.get_name()
        proc.create()

        # Initialize workspace directory structure
        logging.info("Initializing workspace for task '%s' on processor '%s'..." % (task_id, proc_name))

        # Create working directories only visible to processor
        proc.mkdir(workspace.get_wrk_dir())
        proc.mkdir(workspace.get_log_dir())
        proc.set_wrk_dir(workspace.get_wrk_dir())
        proc.set_log_dir(workspace.get_log_dir())

        # Create output directories visible to entire platform
        self.mkdir(workspace.get_tmp_output_dir())
        self.mkdir(workspace.get_final_output_dir())
        self.mkdir(workspace.get_final_log_dir())

        # Make the entire workspace directory accessible to everyone
        logging.debug("Updating workspace permissions on processor '%s'..." % proc.get_name())
        cmd = "sudo chmod -R 777 %s" % self.wrk_dir
        proc.run(job_name="update_wrkspace_perms", cmd=cmd)

        # Wait for all the above commands to complete
        proc.wait()

        # Load task inputs (remote files, docker, etc.)
        logging.info("Loading inputs into workspace for task '%s' on processor '%s'..." % (task_id, proc_name))
        self.__load_inputs()

        # Update workspace permissions again
        logging.info("Final permission update for task '%s' on processor '%s'..." % (task_id, proc_name))
        cmd = "sudo chmod -R 777 %s" % self.workspace.get_wrk_dir()
        proc.run("update_wrk_dir_perms", cmd)

        # Wait for everything to finish on processor
        proc.wait()
        logging.info("Successfully loaded processor (%s) for task '%s'!" % (proc_name, task_id))

    def __load_input_data(self, proc, input_files):

        # Load input files
        seen = []
        for input_file in input_files:

            if input_file.is_remote():

                # Transfer entire containing directory if one is specified
                if input_file.get_containing_dir() is not None:
                    src_path = input_file.get_containing_dir()

                # Transfer single file otherwise
                else:
                    src_path = input_file.get_path()

                # Add wildcard string if path is a prefix
                if input_file.is_prefix():
                    # Transfer with wildcard if path provided is a prefix
                    src_path += "*"

                # Transfer to resource directory
                if src_path not in seen:
                    logging.debug("Transferring remote file '%s' with path %s to processor %s..." % (input_file.get_file_id(), src_path, proc))
                    proc.transfer(src_path=src_path,
                                  dest_dir=proc.get_workspace_dir(),
                                  log_transfer=True,
                                  job_name="transfer_%s" % input_file.get_file_id())

                # Add src path to list of seen paths
                seen.append(src_path)

                # Update path to reflect transfer
                input_file.update_path(new_dir=proc.get_workspace_dir())
                logging.debug("Updated path: %s" % input_file.get_path())

    def get_config(self):
        return self.config

    def get_max_nr_cpus(self):
        return self.MAX_NR_CPUS

    def get_max_mem(self):
        return self.MAX_MEM

    def get_final_output_dir(self):
        return self.final_output_dir

    def get_wrk_dir(self):
        return self.wrk_dir

    def lock(self):
        with self.platform_lock:
            self.__locked = True

    def unlock(self):
        with self.platform_lock:
            self.__locked = False

    ####### ABSTRACT METHODS TO BE IMPLEMENTED BY INHERITING CLASSES
    @abc.abstractmethod
    def init_task_processor(self, name, nr_cpus, mem, disk_space):
        # Return a processor object with given resource requirements
        pass

    @abc.abstractmethod
    def mkdir(self, dir_path):
        # Make a directory if it doesn't already exists
        pass

    @abc.abstractmethod
    def path_exists(self, path):
        # Determine if a path exists either locally on platform or remotely
        pass

    @abc.abstractmethod
    def handle_report(self, report):
        pass

    ####### PRIVATE UTILITY METHODS
    @staticmethod
    def generate_unique_id(id_len=6):
        return str(uuid.uuid4())[0:id_len]

    @staticmethod
    def standardize_dir(dir_path):
        # Makes directory names uniform to include a single '/' at the end
        return dir_path.rstrip("/") + "/"
