import os
import logging
import abc

from IO import PipelineFile, PlatformFile, PlatformFileSet

class Platform(object):
    # Abstract base class representing a GAP cloud computing platform
    # Can be extended to support multiple cloud computing services (e.g. AWS, GoogleCloud)

    __metaclass__ = abc.ABCMeta

    def __init__(self, name, config_file):

        # Platform name
        self.name = name

        # Config dictionary containing platform specifications
        self.config         = self.get_platform_config(config_file)

        # Set common directories as attributes so we don't have to type massively long variable names
        self.wrk_dir                        = self.config["paths"]["wrk_dir"]
        self.log_dir                        = self.config["paths"]["log_dir"]
        self.tmp_dir                        = self.config["paths"]["tmp_dir"]
        self.resource_dir                   = self.config["paths"]["resource_dir"]
        self.bin_dir                        = self.config["paths"]["bin_dir"]
        self.output_dir                     = self.config["paths"]["output_dir"]

        # Boolean for whether platform has been launched
        self.launched   = False

        # Instances managed by the platform
        self.instances      = {}
        self.main_instance  = None

        # Files managed by the platform
        self.files              = None
        self.output_files       = PlatformFileSet()

    def get_platform_config(self, config_file):
        # Reads platform specifications from an external config file
        # Converts all items in 'path' section to PlatformFile objects
        config = self.parse_config(config_file)
        for file_name, file_info in config["paths"]:
            file_path = file_info.pop("path")
            config["paths"][file_name] = PlatformFile(file_name, file_path, **file_info)
        return config

    def launch_platform(self, input_files, **kwargs):
        # Readies platform for analysis

        # Set list of input files
        self.files = input_files

        # Check remote input file existence before launch
        self.validate_platform()

        # Create main instance
        main_instance_name = self.generate_main_instance_name()
        self.create_instance(main_instance_name, is_main_instance=True, **kwargs)

        # Specify that main instance has launched
        self.launched = True

        # Initialize directory structure on main instance
        self.make_dirs()

        # Transfer any remote files to main instance
        self.upload_remote_files()

        # Create symbolic links of executables in bin dir
        self.link_executables_to_bin_dir()

        # Make everything in working directory readable/writeable/executable to anyone
        job_name = "chmod_wrk_dir_777"
        cmd = "sudo chmod -R %s %s !LOG3!" % ("777", self.wrk_dir)
        self.main_instance.run_command(job_name, cmd, proc_wait=True)

        # Do one last validation to make sure all input files actually exist locally on the main instance
        self.validate_platform()

    def create_instance(self, instance_name, is_main_instance=False, **kwargs):
        # Wrapper function used for creation of any instance
        # Creates main/split instances with a given name, adds them to list of platform instances, and waits for the to create
        if is_main_instance:
            instance = self.init_main_instance(instance_name, **kwargs)
        else:
            kwargs["main_server"] = kwargs.get("main_server", self.main_instance.name)
            instance = self.init_split_instance(instance_name, **kwargs)

        # set value of main instance attribute if main instance
        if is_main_instance:
            self.instances[instance_name] = instance
            self.main_instance            = self.instances[instance_name]
        else:
            self.instances[instance_name] = instance

        # Create instance and wait for it to complete
        instance.create()
        instance.wait_process("create")
        return instance

    def make_dirs(self):
        # Create directory structure on main instance
        # Creates all local directories specified in the platform config
        for path_name, path in self.config["paths"].iteritems():
            if path.is_dir() and not path.is_remote_path():
                cmd = "mkdir -p %s" % path
                job_name = "mkdir_%s" % path_name
                self.main_instance.run_command(job_name, cmd)

        self.main_instance.wait_all()

    def upload_remote_files(self):
        # Uploads all remote input files to main instance
        if self.files is None:
            return

        for input_file in self.files:
            if input_file.is_remote_path():
                # Upload resources to resource diretory, everything else to work directory
                dest_dir = self.wrk_dir if isinstance(input_file, PipelineFile) else self.wrk_dir
                job_name = "transfer_%s" % (input_file.get_name())
                self.upload_remote_file(job_name, input_file, dest_dir, recursive=True)
                self.update_file_path(input_file, dest_dir)

        # Wait for files to finish copying
        self.main_instance.wait_all()

    def upload_remote_file(self, job_name, remote_src_file, local_dest_dir, recursive=True):
        # Upload a remote file to a local platform directory
        if remote_src_file.get_containing_dir() is not None:
            source_path = remote_src_file.get_containing_dir()
        elif remote_src_file.is_basename():
            source_path = "%s*" % remote_src_file.get_path()
        else:
            source_path = remote_src_file.get_path()
        self.main_instance.transfer(job_name, source_path, local_dest_dir, recursive)

    def link_executables_to_bin_dir(self):
        # Create softlinks of all executable files in bin dir
        if self.files is None:
            return

        for input_file in self.files:
            # Create softlinks for any executables in the bin dir
            if input_file.is_executable():
                basename    = os.path.basename(input_file.get_path())
                link_name   = os.path.join(self.bin_dir, basename)
                cmd         = "ln -s %s %s" % (input_file, link_name)
                job_name    = "softlink_to_bin_%s_%d" % (input_file.get_name())
                self.main_instance.run_command(job_name, cmd)

        # Wait for processes to complete
        self.main_instance.wait_all()

    def validate_platform(self, **kwargs):
        # Validate platform before or after launch

        # Validate output directory before launch
        if not self.launched:
            self.validate_output_dir(**kwargs)

        errors = False
        # Validate that all platform input files exist
        for platform_file in self.files:
            file_name = platform_file.get_file_name()
            file_path = platform_file.get_file_path()
            if not self.launched and platform_file.is_remote_file():
                # Pre-launch validation: Check that remote files exist
                logging.info("Checking existence of remote file '%s'..." % file_name)
                if not self.remote_file_exists(platform_file):
                    logging.error("Remote file %s not found with the following path: %s. "
                                  "Please provide a valid path in the config/sample sheet." %
                                  (file_name, file_path))
                    errors = True

            elif self.launched:
                # Post-launch validation: Check that local files exist
                logging.info("Checking existence of file '%s' on main instance..." % file_name)
                job_name = "checkExists_main_inst_%s" % file_name
                if not self.main_instance.file_exists(job_name, platform_file):
                    logging.error("%s not found on the main instance with the following path: %s. "
                                  "Please provide a valid path in the config/sample sheet." % (
                                  file_name, file_path))
                    errors = True
        if errors:
            raise IOError("One or more input files were missing from the platform. See above for details!")

    def add_output_file(self, output_file, output_sub_dir=None):
        # Register a new file to be returned on platform exit
        # Optionally specify the name of a subdir where file will be stored within the platform output_dir
        output_file.add_metadata(key="output_sub_dir", value=output_sub_dir)
        self.output_files.append(output_file)

    def finalize(self):
        # Copy final output files from pipeline to cloud storage system
        # Nothing to copy if the main-server does not exist
        if self.main_instance is None:
            return

        try:
            # Transfer all files marked as output files
            for platform_file in self.output_files:
                    file_path       = platform_file.get_path()
                    file_name       = platform_file.get_name()
                    dest_dir        = self.output_dir
                    if platform_file.has_metadata("output_sub_dir"):
                        # Check to see if the file should be transferred to a subdirectory within output_dir
                        dest_dir = os.path.join(self.output_dir, platform_file.get_metadata("output_sub_dir"))
                        dest_dir = "%s/" % dest_dir.rstrip("/")

                    # Transfer file
                    job_name   = "copy_output_%s" % file_name
                    self.main_instance.transfer(job_name=job_name,
                                                source_path=file_path,
                                                dest_path=dest_dir,
                                                recursive=True)

                    # Update output file path to reflect transfer to output_dir
                    self.update_file_path(src_path=file_path, dest_dir=dest_dir)

            # Wait for all transfers to finish
            self.main_instance.wait_all()

        except BaseException as e:
            if e.message != "":
                logging.error(
                    "Could not copy the final output to the cloud storage system! The following error appeared: %s." % e.message)
            else:
                logging.error("Could not copy the final output to the cloud storage system!")

        # Copy the logs
        try:
            self.main_instance.transfer("copyLogs",
                                        source_path=self.log_dir,
                                        dest_path=self.output_dir,
                                        log_transfer=False)
            self.main_instance.wait_process("copyLogs")

        except BaseException as e:
            if e.message != "":
                logging.error("Could not copy the logs to the cloud storage system! The following error appeared: %s." % e.message)
            else:
                logging.error("Could not copy the logs to the cloud storage system!")

    @abc.abstractmethod
    def parse_config(self, config_file):
        # Base method to parse and validate config_file containing platform parameters
        # Should returns a dictionary-like object
        raise NotImplementedError(
            "Platform does not have a required \"parse_config()\" method!")

    @abc.abstractmethod
    def generate_main_instance_name(self, **kwargs):
        # Base method to generate the name of the platform's main instance
        # In theory should do some sort of checking to make sure the main instance name is valid
        raise NotImplementedError(
            "Platform does not have a required \"generate_main_instance_name()\" method!")

    @abc.abstractmethod
    def generate_split_instance_name(self, tool_id, module_name, split_id, **kwargs):
        # Base method to generate the name of the platform's main instance
        # In theory should do some sort of checking to make sure the main instance name is valid
        raise NotImplementedError(
            "Platform does not have a required \"generate_split_instance_name()\" method!")

    @abc.abstractmethod
    def init_main_instance(self, instance_name, **kwargs):
        # Abstract method to create the main instance that will run the pipeline
        raise NotImplementedError(
            "Platform does not have a required \"init_main_instance()\" method!")

    @abc.abstractmethod
    def init_split_instance(self, instance_name, **kwargs):
        # Abstract method that should return an instance object that will be used to run split jobs
        raise NotImplementedError(
            "Platform does not have a required \"init_split_instance()\" method!")

    @abc.abstractmethod
    def remote_file_exists(self, remote_file, **kwargs):
        raise NotImplementedError(
            "Platform does not have a required \"remote_file_exists()\" method!")

    @abc.abstractmethod
    def validate_output_dir(self, **kwargs):
        raise NotImplementedError(
            "Platform does not have a required \"validate_output_dir()\" method!")

    @abc.abstractmethod
    def clean_up(self):
        # Base method to delete all cloud resources that were created during runtime.
        # Run at the end of each pipeline regardless of success or fail.
        # Should be extended by inheriting classes
        raise NotImplementedError(
            "Platform does not have a required \"clean_up()\" method!")

    def get_name(self):
        return self.name

    def get_config(self):
        return self.config

    def get_main_instance(self):
        return self.main_instance

    def get_instance(self, instance_name):
        return self.instances[instance_name]

    def get_wrk_dir(self):
        return self.wrk_dir

    def get_log_dir(self):
        return self.log_dir

    def get_tmp_dir(self):
        return self.tmp_dir

    def get_resource_dir(self):
        return self.resource_dir

    def get_bin_dir(self):
        return self.bin_dir

    def get_output_dir(self):
        return self.output_dir

    def set_output_dir(self, path):
        self.output_dir.set_path(str(path))

    def set_wrk_dir(self, path):
        self.wrk_dir.set_path(str(path))

    def set_log_dir(self, path):
        self.log_dir.set_path(str(path))

    def set_tmp_dir(self, path):
        self.tmp_dir.set_path(str(path))

    def set_resource_dir(self, path):
        self.resource_dir.set_path(str(path))

    def set_bin_dir(self, path):
        self.bin_dir.set_path(str(path))

    @staticmethod
    def update_file_path(src_path, dest_dir):
        # Silently updates a file path after transferring a file from one directory to another
        if src_path.get_containing_dir() is None:
            # Case: File transferred directly to dest_dir
            basename = src_path.get_path().split("/")[-1]
            local_path = os.path.join(dest_dir, basename)
        else:
            # Case: File is contained within a directory which was transferred to dest_dir
            sub_dir = src_path.get_containing_dir().split("/")[-1]
            rel_path = os.path.relpath(src_path.get_path(), src_path.get_containing_dir())
            local_path = os.path.join(dest_dir, sub_dir, rel_path)
        src_path.set_path(local_path)




