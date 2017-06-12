import os
import logging
import abc

class Platform(object):
    # Abstract base class representing a GAP cloud computing platform
    # Can be extended to support multiple cloud computing services (e.g. AWS, GoogleCloud)

    __metaclass__ = abc.ABCMeta

    def __init__(self, config, pipeline_data):

        # Config dictionary containing platform specifications
        self.config         = config

        # Object containing input files that need to be transferred to main server for each sample
        self.pipeline_data  = pipeline_data

        # Set common directories as attributes so we don't have to type massively long variable names
        self.wrk_dir                        = self.config["paths"]["instance_wrk_dir"]
        self.log_dir                        = self.config["paths"]["instance_log_dir"]
        self.tmp_dir                        = self.config["paths"]["instance_tmp_dir"]
        self.tool_dir                       = self.config["paths"]["instance_tool_dir"]
        self.resource_dir                   = self.config["paths"]["instance_resource_dir"]
        self.bin_dir                        = self.config["paths"]["instance_bin_dir"]
        self.cloud_storage_tool_dir         = self.config["paths"]["cloud_storage_tool_dir"]
        self.cloud_storage_resource_dir     = self.config["paths"]["cloud_storage_resource_dir"]
        self.cloud_storage_output_dir       = self.config["paths"]["cloud_storage_output_dir"]

        # Boolean for whether platform has been launched
        self.launched   = False

        # Instances managed by the platform
        self.instances      = {}
        self.main_instance  = None

        # Create name for cloud output directory for returning results
        output_subdirectory = "%s/" % self.pipeline_data.pipeline_name.rstrip("/")
        self.cloud_storage_output_dir = os.path.join(self.cloud_storage_output_dir, output_subdirectory)

    def launch_platform(self, **kwargs):
        # Checks and launches main instance where pipeline will be executed

        # Check to make sure cloud storage files specified in config actually exist on cloud storage
        self.validate_cloud_storage_files()

        # Launch main instance
        main_instance_name = self.generate_main_instance_name()
        self.create_instance(main_instance_name, is_main_instance=True, **kwargs)

        # Transfer input data to main instance
        self.transfer_data()

        # Update filenames in config/sample data to reflect their new location on main instance
        self.update_file_paths()

        # Place symbolic links for all tools in a directory that appears in the global $PATH
        # Allows tools to access their dependencies
        self.add_tools_to_path()

        # Give read/write/execute access to any file/subdirectory in working directory
        self.change_permissions()

        # Check that all necessary files specified in config actually exist on main instance
        self.validate_main_instance_files()

    def make_dirs(self):
        # Create directory structure on main instance

        logging.info("(%s) Creating the necessary directories on the instance." % self.main_instance.name)

        # Create commands
        commands = {
            "log":     self.log_dir,
            "tmp":     self.tmp_dir,
            "tool":    self.tool_dir,
            "res":     self.resource_dir,
            "bin":     self.bin_dir,
        }

        # Create directories
        for dir_name, dir_path in commands.iteritems():
            proc_name = "create%sDir" % dir_name.capitalize()
            proc_cmd  = "mkdir -p %s" % dir_path
            self.main_instance.run_command(proc_name, proc_cmd, log=False)

        # Wait for all directory creations
        self.main_instance.wait_all()

    def transfer_data(self):
        # Transfer necessary files specified in config/sample sheet to main instance

        # Transfer cloud tool directory to main instance
        if self.cloud_storage_tool_dir is not None:
            tools = "%s*" % self.cloud_storage_tool_dir
            self.main_instance.transfer(job_name="copyTools",
                                                   source_path=tools,
                                                   dest_path=self.tool_dir,
                                                   recursive=True,
                                                   log_transfer=True)

        # Transfer cloud resource directory to main instance
        if self.cloud_storage_resource_dir is not None:
            resources = "%s*" % self.cloud_storage_resource_dir
            self.main_instance.transfer(job_name="copyResources",
                                                   source_path=resources,
                                                   dest_path=self.resource_dir,
                                                   recursive=True,
                                                   log_transfer=True)

        # Transfer sample input data to main instance
        for sample_name, sample in self.pipeline_data.get_samples().iteritems():
            # Get sample data for next sample
            sample_data = sample.input_data
            for file_type, input_file in sample_data.iteritems():
                # Transfer all sample input files to main instance
                job_name = "copy_%s_%s" % (sample_name, file_type)
                self.main_instance.transfer(job_name=job_name,
                                                       source_path=input_file,
                                                       dest_path=self.wrk_dir,
                                                       recursive=True,
                                                       log_transfer=True)
        self.main_instance.wait_all()

    def update_file_paths(self):
        # Updates filenames in config/sample sheet after they've been transferred to main instance

        # Update tool filenames
        for file_type, file_path in self.config["paths"]["tools"].iteritems():
            # Update path if transferred from cloud to instance
            if file_path.startswith(self.cloud_storage_tool_dir):
                self.config["paths"]["tools"][file_type] = file_path.replace(self.cloud_storage_tool_dir, self.tool_dir)

        # Update resource filenames
        for file_type, file_path in self.config["paths"]["resources"].iteritems():
            # Update path if transferred from cloud to instance
            if file_path.startswith(self.cloud_storage_resource_dir):
                self.config["paths"]["resources"][file_type] = file_path.replace(self.cloud_storage_resource_dir, self.resource_dir)

        # Update names of sample input files
        for sample_name, sample in self.pipeline_data.get_samples().iteritems():
            # Get sample data for next sample
            sample_data = sample.input_data
            for file_type, input_file in sample_data.iteritems():
                    # Update file name to point to file on main instance
                    basename = input_file.split("/")[-1]
                    sample.input_data[file_type] = os.path.join(self.wrk_dir, basename)

    def add_tools_to_path(self):
        # Make symbolic links in the bin directory for all exectuables
        # If bin_dir is found in an instances global $PATH, tools can now be referenced from anywhere
        for tool_type, tool_path in self.config["paths"]["tools"].iteritems():
            basename = tool_path.split("/")[-1]
            link_name = os.path.join(self.bin_dir, basename)
            cmd = "ln -s %s %s" % (tool_path, link_name)
            self.main_instance.run_command("softlink_%s" % tool_type, cmd)
        self.main_instance.wait_all()

    def change_permissions(self):
        # Set permissions to read/write/execute for all files/dirs in main instance working dir
        cmd = "sudo chmod -R 777 %s !LOG3!" % self.wrk_dir
        self.main_instance.run_command("changeDirPermissions", cmd, proc_wait=True)

    def validate_main_instance_files(self):
        # Validate that all necessary files actually exist on the main instance

        # Check tools files to see whether they exist on main instance
        logging.info("Verifying that all tool files in config actually exist on the main instance...")
        for file_type, file_name in self.config["paths"]["tools"].iteritems():
            self.validate_main_instance_file(file_type, file_name)

        # Check resources files to see whether they exist on main instance
        logging.info("Verifying that all resource files in config actually exist on the main instance...")
        for file_type, file_name in self.config["paths"]["resources"].iteritems():
            self.validate_main_instance_file(file_type, file_name)

            # Remove any wildcard characters if they exist.
            # This will only be the case for index files which share a basename that needs to be passed to a specific tool.
            self.config["paths"]["resources"][file_type] = file_name.rstrip("*")

        # Check that all sample data files actually made it onto main instance
        logging.info("Verifying that all sample input data files actually exist on main instance...")
        for sample_name, sample in self.pipeline_data.get_samples().iteritems():

            logging.info("Checking input files for sample: %s" % sample_name)

            # Get sample data for next sample
            sample_data = sample.input_data
            for file_type, file_name in sample_data.iteritems():
                # Check that input file exists
                file_type = "%s_%s" % (sample_name, file_type)
                self.validate_main_instance_file(file_type, file_name)

    def validate_cloud_storage_files(self):
        # Validate that all necessary cloud storage files actually exist on cloud storage before launch

        # Check that cloud storage output directory is a valid directory that can be written to
        logging.info("Checking that cloud storage output directory is a valid path...")
        self.validate_cloud_output_directory()

        # Validate tool files to be transferred from cloud storage to main instance upon launch
        if self.cloud_storage_tool_dir is not None:

            # Check that tool directory actually exists on cloud storage
            self.validate_cloud_storage_file("Cloud Storage Tool Directory", self.cloud_storage_tool_dir)

            # Check that cloud storage tool files appear in the cloud tool directory specified in config
            logging.info("Checking whether cloud storage tools are found in cloud tool directory...")
            for file_type, file_name in self.config["paths"]["tools"].iteritems():

                # Check whether tool path is a unix-style absolute path
                # Assumes non-unix paths in config need to be transferred from cloud storage
                if not self.is_unix_absolute_path(file_name):
                    self.validate_cloud_storage_file(file_type, file_name,
                                                     required_dir=self.cloud_storage_tool_dir)

        # Validate resource files to be transferred from cloud storage to main instance upon launch
        if self.cloud_storage_resource_dir is not None:

            # Check that resource directory actually exists on cloud storage
            self.validate_cloud_storage_file("Cloud Storage Resource Directory", self.cloud_storage_resource_dir)

            # Check that cloud storage resource files appear in the cloud resources directory specified in config
            logging.info("Checking resource files to be transferred from cloud storage to main instance on startup...")
            for file_type, file_name in self.config["paths"]["resources"].iteritems():

                if not self.is_unix_absolute_path(file_name):
                    self.validate_cloud_storage_file(file_type, file_name,
                                                     required_dir=self.cloud_storage_resource_dir)

        # Check sample input files from sample sheet JSON
        logging.info("Checking whether sample input files exist on cloud storage system...")
        for sample_name, sample in self.pipeline_data.get_samples().iteritems():

            logging.info("Checking input files for sample: %s" % sample_name)

            # Get sample data for next sample
            sample_data = sample.input_data
            for file_type, file_name in sample_data.iteritems():
                # Check that input file exists
                file_type = "%s_%s" % (sample_name, file_type)
                self.validate_cloud_storage_file(file_type, file_name)

    def validate_main_instance_file(self, file_type, file_name):
        # Check to see if instance file exists if main instance, throws error otherwise
        # Throws error if attempt to check existence of file before main server launches

        if not self.launched:
            logging.error("Attempted to check existence of file %s on main instance before main instance has been launched!")
            exit(1)

        logging.info("Checking existence of %s on main instance with path: %s..." % (file_type, file_name))
        job_name = "checkExists_%s" % file_type

        if not self.main_instance.file_exists(job_name, file_name):
            logging.error("%s not found on main instance with the following path: %s. "
                          "Please provide a valid path in the config/sample sheet." % (file_type, file_name))

            raise IOError("One or more file paths specified in the config/sample sheet do not exist on the main instance! "
                          "See error message above!")

    def validate_cloud_storage_file(self, file_type, file_name, required_dir=None):
        # Check to see if a file exists on a cloud storage system
        # Throws error if file doesn't exist or, optionally, if file isn't located in the required_dir

        if required_dir is None:
            logging.info("Checking existence of '%s': %s" % (file_type, file_name))
        else:
            logging.info("Checking whether '%s' (%s) is in dir: %s" % (file_type, file_name, required_dir))

        # Check to see if file exists on cloud storage system
        if self.file_exists_on_cloud_storage(file_name):

            if (required_dir is not None) and not file_name.startswith(required_dir):
                logging.error("'%s' is present on the cloud storage system (%s) but is not located in the expected directory: %s." \
                              % (file_type, file_name, required_dir))
                exit(1)

        else:
            logging.error("%s not found on the cloud storage with the following path: %s. "
                          "Please provide valid path in config/sample sheet." % (file_type, file_name))
            exit(1)

    def finalize(self):
        # Copy final output files from pipeline to cloud storage system
        # Nothing to copy if the main-server does not exist
        if self.main_instance is None:
            return

        final_output = self.pipeline_data.get_final_output()
        try:
            # Copy the available final_outputs returned by the pipeline so far
            for module_name in final_output:
                for tool_id in final_output[module_name]:
                    # Get list of output files for the next node
                    output_files = final_output[module_name][tool_id]
                    if len(output_files) > 1:
                        # Copy files to module-specific directory if module has >1 output file
                        dest_dir = "%s%s/" % (self.cloud_storage_output_dir, module_name)
                    else:
                        dest_dir = self.cloud_storage_output_dir

                    # Copy output files to destination directory
                    for i in range(len(output_files)):
                        output_file_type    = output_files[i][0]
                        output_file         = output_files[i][1]

                        # Check to make sure output file is not empty
                        if (isinstance(output_file, basestring)) and (output_file != ""):
                            # Transfer output file to output directory
                            transfer_name = "copyOut_%s_%s_%s_%d" % (tool_id, module_name, output_file_type, i)
                            self.main_instance.transfer(transfer_name,
                                                        source_path=output_file,
                                                        dest_path=dest_dir,
                                                        recursive=True)
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
                                                  dest_path=self.cloud_storage_output_dir,
                                                  log_transfer=False)
            self.main_instance.wait_process("copyLogs")

        except BaseException as e:
            if e.message != "":
                logging.error("Could not copy the logs to the cloud storage system! The following error appeared: %s." % e.message)
            else:
                logging.error("Could not copy the logs to the cloud storage system!")

    def create_instance(self, instance_name, is_main_instance=False, **kwargs):
        # Wrapper function used for creation of any instance
        # Creates main/split instances with a given name, adds them to list of platform instances, and waits for the to create
        if is_main_instance:
            instance = self.init_main_instance(instance_name, **kwargs)
        else:
            kwargs["main_server"] = kwargs.get("main_server", self.main_instance.name)
            instance = self.init_split_instance(instance_name, **kwargs)

        # Store the instance object
        self.instances[instance_name] = instance

        # set value of main instance attribute if main instance
        if is_main_instance:
            self.main_instance = instance

        # Create instance and wait for it to complete
        instance.create()
        instance.wait_process("create")

        # Mark as launched
        self.launched = True

        # Run the startup tasks
        self.startup_tasks(instance, is_main_instance=is_main_instance)

        # Initialize directory structure on main instance
        if is_main_instance:
            self.make_dirs()

    def get_config(self):
        return self.config

    def get_pipeline_data(self):
        return self.pipeline_data

    def get_main_instance(self):
        return self.main_instance

    def startup_tasks(self, instance, is_main_instance=False):
        pass

    @abc.abstractmethod
    def generate_main_instance_name(self):
        # Base method to generate the name of the platform's main instance
        # In theory should do some sort of checking to make sure the main instance name is valid
        raise NotImplementedError(
            "Platform does not have a required \"generate_main_instance_name()\" method!")

    @abc.abstractmethod
    def generate_split_instance_name(self, tool_id, module_name, split_id):
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
    def validate_cloud_output_directory(self):
        raise NotImplementedError(
            "Platform does not have a required \"validate_cloud_storage_output_directory()\" method!")

    @abc.abstractmethod
    def file_exists_on_cloud_storage(self, file_name):
        raise NotImplementedError(
            "Platform does not have a required \"check_cloud_file_exists()\" method!")

    @abc.abstractmethod
    def clean_up(self):
        # Base method to delete all cloud resources that were created during runtime.
        # Run at the end of each pipeline regardless of success or fail.
        # Should be extended by inheriting classes
        raise NotImplementedError(
            "Platform does not have a required \"clean_up()\" method!")

    @staticmethod
    def is_unix_absolute_path(path):
        return path.startswith("/")





