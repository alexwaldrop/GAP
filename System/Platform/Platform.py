import logging
import os
import abc
import hashlib
import time

from Config import ConfigParser

class Platform(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, name, platform_config_file, final_output_dir):

        # Platform name
        self.name = name

        # Initialize platform config
        config_spec_file    = self.define_config_spec_file()
        config_parser       = ConfigParser(platform_config_file, config_spec_file)
        self.config         = config_parser.get_config()

        # Init platform variables from config
        self.MAX_NR_CPUS        = self.config["PROC_MAX_NR_CPUS"]
        self.MAX_MEM            = self.config["PROC_MAX_MEM"]

        # Define workspace directory names
        wrk_dir                 = self.config["workspace_dir"]
        self.workspace          = self.define_workspace(wrk_dir)
        self.final_output_dir   = self.standardize_dir(final_output_dir)

        # Boolean for whether main processor has been initialized
        self.launched   = False

        # Dictionary to hold processors currently managed by the platform
        self.processors = {}

        # Main platform processor
        self.main_processor = None

    def define_workspace(self, workspace_dir):
        # Define workspace directory and final output paths
        dirs = {"wrk" : self.standardize_dir(workspace_dir)}
        for sub_dir in ["log", "tmp", "res", "bin", "lib"]:
            dirs[sub_dir] = self.standardize_dir(os.path.join(workspace_dir, sub_dir))
        return dirs

    def launch_platform(self, resource_kit, sample_set):

        # Loads platform capable of running pipeline
        self.main_processor = self.get_main_processor()
        self.launched = True

        # Initialize workspace directory structure
        logging.info("Initializing workspace env...")
        self.init_workspace()
        logging.info("Workspace successfully initialized!")

        # Create final output directory and wait to make sure it was actually created
        logging.info("Creating final output directory...")
        self.mkdir(self.final_output_dir)
        self.main_processor.wait()
        logging.info("Final output directory successfully create!")

        # Transfer remote resources to platform resource directory
        # Link exectuable files to workspace bin directory
        # Link library files to workspace lib directory
        self.install_resource_kit(resource_kit)

        # Load input data on platform workspace
        self.load_input_data(sample_set)

        # Make everything in the workspace accessible to everyone
        logging.info("Updating workspace permissions...")
        cmd = "sudo chmod -R 777 %s" % self.get_workspace_dir()
        self.run_quick_command("update_wrkspace_perms", cmd)

    def install_resource_kit(self, resource_kit):
        # Transfers remote resources to workspace resource directory
        # Links exectuables/libraries to wrkspace bin and lib directories
        logging.info("Installing resource kit to workspace...")
        resource_dir  = self.get_workspace_dir("res")
        bin_dir       = self.get_workspace_dir("bin")
        lib_dir       = self.get_workspace_dir("lib")

        # Get list of resources
        resources = resource_kit.get_resources()

        # For every resource:
        # Transfer to platform if remote
        for resource_type, resource_names in resources.iteritems():
            for resource_name, resource in resource_names.iteritems():
                if resource.is_remote():

                    # Transfer entire containing directory if one is specified
                    if resource.get_containing_dir() is not None:
                        src_path = resource.get_containing_dir()

                    # Transfer single file otherwise
                    else:
                        src_path = resource.get_path()

                    # Add wildcard string if path is a prefix
                    if resource.is_prefix():
                        # Transfer with wildcard if path provided is a prefix
                        src_path += "*"

                    # Transfer to resource directory
                    logging.info("Transferring remote resource '%s' with path %s..." % (resource_name, src_path))
                    self.transfer(src_path=src_path,
                                  dest_dir=resource_dir,
                                  log_transfer=True,
                                  job_name="transfer_%s" % resource_name)

                    # Update path to reflect transfer
                    src_path = src_path.replace("*", "")
                    resource_kit.update_path(src_path, resource_dir)
                    logging.info("Updated path: %s" % resource.get_path())

        # Wait for all transfers to complete
        self.main_processor.wait()

        # Link bin/lib directories if necessary
        for resource_type, resource_names in resources.iteritems():
            for resource_name, resource in resource_names.iteritems():
                # Link executable to workspace bin dir if executable
                if resource.is_executable():
                    logging.info("Linking executable resource '%s' to workspace bin directory..." % resource_name)
                    self.__link_path(resource.get_path(), bin_dir)

                if resource.is_library():
                    logging.info("Linking library resource '%s' to workspace lib directory..." % resource_name)
                    self.__link_path(resource.get_path(), lib_dir)
        logging.info("Resource kit successfully installed!")

    def load_input_data(self, sample_set):
        # Transfer sample input data files to platform workspace
        # Get paths to transfer to workspace directory
        logging.info("Transferring sample input data to workspace...")
        paths       = sample_set.get_paths()
        dest_dir    = self.get_workspace_dir("wrk")
        count = 1
        for path_type, path_data in paths.iteritems():
            if isinstance(path_data, list):
                for path in path_data:
                    # Transfer file to workspace
                    logging.info("Transferring sample file: %s" % path)
                    job_name = "transfer_user_input_%d" % count
                    self.transfer(src_path=path,
                                  dest_dir=dest_dir,
                                  log_transfer=True,
                                  job_name=job_name)
                    # Update path to reflect transfer
                    sample_set.update_path(path, dest_dir)
                    count += 1
            else:
                path = path_data
                logging.info("Transferring sample file: %s" % path)
                # Transfer file to workspace
                job_name = "transfer_user_input_%d" % count
                self.transfer(src_path=path,
                              dest_dir=dest_dir,
                              log_transfer=True,
                              job_name=job_name)
                # Update path to reflect transfer
                sample_set.update_path(path, dest_dir)
                count += 1

        # Wait for all transfers to complete
        self.main_processor.wait()
        logging.info("Input data successfully transferred to workspace!")

    def get_main_processor(self):
        # Create and return a main processor that's ready to run commands
        logging.info("Creating main processor...")
        main_processor = self.create_main_processor()

        # Add to list of processors if not already there
        if main_processor.name not in self.processors:
            self.processors[main_processor.name] = main_processor

        # Set log directory if not already done
        main_processor.set_log_dir(self.get_workspace_dir("log"))

        # Add workspace bin directory to PATH env variable
        main_processor.set_env_variable("PATH", self.get_workspace_dir("bin"))

        # Add workspace lib directory to LD_LIB_PATH env variable
        main_processor.set_env_variable("LD_LIB_PATH", self.get_workspace_dir("lib"))

        logging.info("Main processor '%s' ready to load platform!" % main_processor.name)
        return self.processors[main_processor.name]

    def get_processor(self, name, nr_cpus, mem):
        # Ensure unique name for processor
        name        = "%s-%s" % (name, self.generate_unique_id())
        logging.info("Creating processor '%s' with %s CPUs and %s GB of memory" % (name, nr_cpus, mem))

        # Create processor with requested resources (CPU/Mem)
        processor   = self.create_processor(name, nr_cpus, mem)

        # Add to list of processors if not already there
        name = processor.name
        if name not in self.processors:
            self.processors[name] = processor

        # Set log directory
        processor.set_log_dir(self.get_workspace_dir("log"))

        # Add workspace bin directory to PATH env variable
        processor.set_env_variable("PATH", self.get_workspace_dir("bin"))

        # Add workspace lib directory to LD_LIB_PATH env variable
        processor.set_env_variable("LD_LIB_PATH", self.get_workspace_dir("lib"))

        logging.info("Processor '%s' ready for processing!" % name)
        return self.processors[processor.name]

    def run_command(self, job_name, cmd, nr_cpus, mem):
        # Create a processor capable of running a cmd with specified CPU/Mem requirements and run the job
        processor = self.get_processor(job_name, nr_cpus, mem)

        # Begin running process on processor
        processor.run(job_name, cmd)

        # Wait for job to finish and get output
        out, err = processor.wait_process(job_name)

        # Destroy processor
        self.destroy_processor(processor.get_name())

        # Return output
        return out, err

    def run_quick_command(self, job_name, cmd):
        # Run a lightweight command on the main instance.
        # Not intended for jobs requiring more than 1 thread
        self.main_processor.run(job_name, cmd)
        out, err = self.main_processor.wait_process(job_name)
        return out, err

    def return_output(self, output_path, sub_dir=None, dest_file=None, log_transfer=True):
        logging.info("Returning output file: %s" % output_path)
        # Transfer output file to final output directory
        if sub_dir is None:
            self.transfer(src_path=output_path,
                          dest_dir=self.final_output_dir,
                          dest_file=dest_file,
                          log_transfer=log_transfer)
        # Transfer output file to subdirectory within final output directory
        else:
            dest_dir = os.path.join(self.final_output_dir, sub_dir) + "/"
            self.mkdir(dest_dir)
            self.transfer(src_path=output_path,
                          dest_dir=dest_dir,
                          dest_file=dest_file,
                          log_transfer=log_transfer)

    def destroy_processor(self, processor_name):
        self.processors[processor_name].destroy()
        self.processors.pop(processor_name)

    def get_config(self):
        return self.config

    def get_max_nr_cpus(self):
        return self.MAX_NR_CPUS

    def get_max_mem(self):
        return self.MAX_MEM

    def get_final_output_dir(self):
        return self.final_output_dir

    def get_workspace_dir(self, sub_dir=None):
        if sub_dir is None:
            return self.workspace["wrk"]
        else:
            return self.workspace[sub_dir]

    def __link_path(self, src_path, dest_path):
        # Create softlink
        cmd         = "cp -rs %s %s" % (src_path, dest_path)
        job_name    = "linking_file_%s_to_%s" % (os.path.basename(src_path), os.path.basename(dest_path))
        self.main_processor.run(job_name, cmd)

    ####### ABSTRACT METHODS TO BE IMPLEMENTED BY INHERITING CLASSES
    @abc.abstractmethod
    def clean_up(self):
        pass

    @abc.abstractmethod
    def define_config_spec_file(self):
        # Return path to config spec file used to validate platform config
        pass

    @abc.abstractmethod
    def create_processor(self, name, nr_cpus, mem):
        # Return a processor ready to run a process requiring the given amount of CPUs and Memory
        pass

    @abc.abstractmethod
    def create_main_processor(self):
        # Initialize and return the main processor needed to load/manage the platform
        pass

    @abc.abstractmethod
    def init_workspace(self):
        # Create the workspace directories with the main processor
        pass

    @abc.abstractmethod
    def path_exists(self, path):
        # Determine if a path exists either locally on platform or remotely
        pass

    @abc.abstractmethod
    def transfer(self, src_path, dest_dir, dest_file=None, log_transfer=True, job_name=None):
        # Transfer a remote file from src_path to a local directory dest_dir
        # Log the transfer unless otherwise specified
        pass

    @abc.abstractmethod
    def mkdir(self, dir_path):
        # Make a directory if it doesn't already exists
        pass

    ####### PRIVATE UTILITY METHODS
    @staticmethod
    def generate_unique_id(id_len=6):
        return hashlib.md5(str(time.time())).hexdigest()[0:id_len]

    @staticmethod
    def standardize_dir(dir_path):
        # Makes directory names uniform to include a single '/' at the end
        return dir_path.rstrip("/") + "/"
