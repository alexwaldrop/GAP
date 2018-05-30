import logging
import abc
import uuid
import threading

from Config import ConfigParser
from System.Platform import Processor

class TaskPlatformResourceLimitError(Exception):
    pass

class TaskPlatformLockError(Exception):
    pass

class InvalidProcessorError(Exception):
    pass

class Platform(object):
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

        # Check to make sure resource limits are fine
        self.__check_resources()

        # Define workspace directory names
        self.wrk_dir            = self.config["workspace_dir"]
        self.final_output_dir   = self.standardize_dir(final_output_dir)

        # Dictionary to hold processors currently managed by the platform
        self.processors = {}

        # Platform resource threading lock
        self.platform_lock = threading.Lock()

        # Boolean flag to lock processor creation upon cleanup
        self.__locked = False

    def __check_resources(self):
        err = False
        if self.MAX_NR_CPUS > self.TOTAL_NR_CPUS:
            logging.error("Platform config error! Max task cpus (%s) cannot exceed platform cpus (%s)!" % (self.MAX_NR_CPUS, self.TOTAL_NR_CPUS))
            err = True
        elif self.MAX_MEM > self.TOTAL_MEM:
            logging.error("Platform config error! Max task mem (%sGB) cannot exceed platform mem (%sGB)!" % (self.MAX_MEM, self.TOTAL_MEM))
            err = True
        elif self.MAX_DISK_SPACE > self.TOTAL_DISK_SPACE:
            logging.error("Platform config error! Max task disk space (%sGB) cannot exceed platform disk space (%sGB)!" % (self.MAX_DISK_SPACE, self.TOTAL_DISK_SPACE))
            err = True

        if err:
            raise TaskPlatformResourceLimitError("Task resource limit (CPU/Mem/Disk space) cannot exceed platform resource limit!")

    def __get_curr_usage(self):
        # Return total cpus, mem, disk space currently in use on platform
        with self.platform_lock:
            cpu = 0
            mem = 0
            disk_space = 0
            for processor_id, processor in self.processors.iteritems():
                if processor.get_status() > Processor.OFF:
                    cpu += processor.get_cpu()
                    mem += processor.get_mem()
                    disk_space += processor.get_disk_space()
        return cpu, mem, disk_space

    def prepare(self):
        # Perform any platform-specific tasks prior to running pipeline
        pass

    def clean_up(self):
        # Perform any platform-specific tasks after pipeline has run
        pass

    def can_make_processor(self, req_cpus, req_mem, req_disk_space):
        cpu, mem, disk_space = self.__get_curr_usage()
        cpu_overload    = cpu + req_cpus > self.MAX_NR_CPUS
        mem_overload    = mem + req_mem > self.MAX_MEM
        disk_overload   = disk_space + req_disk_space > self.MAX_DISK_SPACE
        return (not cpu_overload) and (not mem_overload) and (not disk_overload) and (not self.__locked)

    def get_processor(self, task_id, nr_cpus, mem, disk_space):
        # Initialize new processor and register with platform

        if self.__locked:
            logging.error("Platform failed to initialize processor with id '%s'! Platform is currently locked!" % task_id)
            raise TaskPlatformLockError("Cannot get processor while platform is locked!")

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

        return self.processors[task_id]

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
