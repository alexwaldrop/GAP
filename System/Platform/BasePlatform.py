import logging
import os
import abc
import uuid
import threading

from Config import ConfigParser

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

        # Current platform resource usage
        self.curr_cpu           = 0
        self.curr_mem           = 0
        self.curr_disk_space    = 0

        # Define workspace directory names
        self.wrk_dir            = self.config["workspace_dir"]
        self.final_output_dir   = self.standardize_dir(final_output_dir)

        # Dictionary to hold processors currently managed by the platform
        self.processors = {}

        # Platform resource threading lock
        self.platform_lock = threading.Lock()

        # Boolean flag to lock processor creation upon cleanup
        self.cleaning_up = False

    def can_run_task(self, task_nr_cpus, task_mem, task_disk_space):
        with self.platform_lock:
            cpu_overload = self.curr_cpu + task_nr_cpus > self.MAX_NR_CPUS
            mem_overload = self.curr_mem + task_mem > self.MAX_MEM
            disk_overload = self.curr_disk_space + task_disk_space > self.MAX_DISK_SPACE
            return (not cpu_overload) and (not mem_overload) and (not disk_overload) and (not self.cleaning_up)

    def get_task_processor(self, task_id, nr_cpus, mem, disk_space, input_files):
        # Create and return a processor that meets resource requirements and

        # Throw error if processor is either too resource intensive or if cleaning up
        if not self.can_run_task(nr_cpus, mem, disk_space):
            logging.error("Cannot create processor for task '%s'! Platform either lacks resources or is cleaning up!" % task_id)
            raise RuntimeError("Attempt to create processor failed due to resource limits or clean-up!")

        proc = self.__get_processor(task_id, nr_cpus, mem, disk_space)

        # Initialize workspace directory structure
        logging.info("Initializing workspace on processor for task '%s' with name '%s'..." % (task_id, proc.get_name()))
        self.__init_proc_workspace(proc, workspace_id=task_id)

        # Localize remote files to workspace directory
        logging.info("Transferring remote inputs for task '%s' to workspace of processor '%s'..." % (task_id, proc.get_name()))
        self.__load_input_data(proc, input_files)

        # Update workspace permissions again
        logging.info("Final permission update on processor '%s' for task '%s'..." % (proc.get_name(), task_id))
        cmd = "sudo chmod -R 777 %s" % proc.get_workspace_dir()
        proc.run("update_wrkspc_perms", cmd)

        # Wait for everything to finish on processor
        proc.wait()
        return proc

    def destroy_task_processor(self, task_id):
        if task_id not in self.processors:
            logging.error("Attempt to destroy non-existant processor! No processor has been created for task with id '%s'!" % task_id)
            raise RuntimeError("Platform cannot destroy non-existant process!")

        # Destroy processor and unlock resources it was using
        proc = self.processors[task_id]
        proc.destroy()
        self.__unlock_resources(proc.nr_cpus, proc.mem, proc.disk_space)

    def __init_proc_workspace(self, proc, workspace_id):

        # Create workspace directories on processor
        workspace = self.__define_task_workspace(workspace_id)

        # Make work directory on processor if it doesn't exist
        proc.mkdir(self.wrk_dir, job_name="mk_wrk_dir")

        # Create workspace subdirectories
        for dir_type in workspace:
            if dir_type != "wrk":
                logging.debug("Creating %s directory on proc '%s'..." % (dir_type, proc.get_name()))
                proc.mkdir(workspace[dir_type], job_name="mk_%s_dir" % dir_type)

        # Make the entire workspace directory accessible to everyone
        logging.debug("Updating workspace permissions on processor '%s'..." % proc.get_name())
        cmd = "sudo chmod -R 777 %s" % self.wrk_dir
        proc.run(job_name="update_wrkspace_perms", cmd=cmd)

        # Wait for all commands to complete
        proc.wait()

        # Set directories on processor
        proc.set_log_dir(workspace["log"])
        proc.set_output_dir(workspace["out"])
        proc.set_tmp_dir(workspace["tmp"])

        logging.debug("Workspace initialized successfully for processor '%s'!" % proc.get_name())

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

    def __define_task_workspace(self, workspace_id):
        # Define workspace directory for a task
        workspace_dir = os.path.join(self.wrk_dir, workspace_id)
        dirs = {"wrk" : self.standardize_dir(workspace_dir)}
        for sub_dir in ["tmp", "out", "log"]:
            dirs[sub_dir] = self.standardize_dir(os.path.join(workspace_dir, sub_dir))
        return dirs

    def __get_processor(self, task_id, nr_cpus, mem, disk_space):
        # Ensure unique name for processor
        name        = "proc-%s-%s-%s" % (self.name[:20], task_id[:25], self.generate_unique_id())
        logging.info("Creating processor '%s' for task '%s'..." % (name, task_id))

        # Create processor with requested resources (CPU/Mem)
        processor   = self.create_processor(name, nr_cpus, mem, disk_space)

        # Add to list of processors if not already there
        if task_id not in self.processors:
            self.processors[task_id] = processor
        else:
            logging.error("Platform cannot create two processors for task with id '%s'!" % task_id)
            raise RuntimeError("Platform attempted to create duplicate task processor!")

        logging.info("Processor '%s' (%d vCPUs, %dGB RAM) ready for processing!" % (name, processor.nr_cpus, processor.mem))
        return self.processors[task_id]

    def __lock_resources(self, cpus, mem, disk_space):
        # Lock cpus, mem, disk space as currently used on platform
        with self.platform_lock:
            self.curr_cpu += cpus
            self.curr_mem += mem
            self.curr_disk_space += disk_space

    def __unlock_resources(self, cpus, mem, disk_space):
        # Unlock resources after task has completed
        with self.platform_lock:
            self.curr_cpu -= cpus
            self.curr_mem -= mem
            self.curr_disk_space -= disk_space
            # Make sure nothing is below 0
            self.curr_cpu = max(self.curr_cpu, 0)
            self.curr_mem = max(self.curr_mem, 0)
            self.curr_disk_space = max(self.curr_disk_space)

    def has_task_processor(self, task_id):
        return task_id in self.processors

    def get_config(self):
        return self.config

    def get_max_nr_cpus(self):
        return self.MAX_NR_CPUS

    def get_max_mem(self):
        return self.MAX_MEM

    def get_final_output_dir(self):
        return self.final_output_dir

    def finalize(self):
        # Lock new processors from launching on platform
        with self.platform_lock:
            self.cleaning_up = True

        # Do any final cleanup
        self.clean_up()

    ####### ABSTRACT METHODS TO BE IMPLEMENTED BY INHERITING CLASSES
    @abc.abstractmethod
    def clean_up(self):
        pass

    @abc.abstractmethod
    def create_processor(self, name, nr_cpus, mem, disk_space):
        # Return a processor ready to run a process requiring the given amount of CPUs and Memory
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
