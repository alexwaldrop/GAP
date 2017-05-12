import os
import subprocess as sp
import logging
import json

from GAP_modules.Google import GoogleException
from GAP_modules.Google import Instance
from GAP_modules.Google import GoogleReadySubscriber
from GAP_modules.Google import GooglePreemptedSubscriber
from GAP_modules.Google import GooglePubSub
from GAP_modules.Google import GoogleLogging

class GoogleCompute(object):

    def __init__(self, config):
        self.config         = config

        # Get google access fields from JSON file
        self.key_location   = self.config["platform"]["service_account_key_file"]
        self.service_acct   = self.get_info_from_key_file(self.key_location, "client_email")
        self.google_project = self.get_info_from_key_file(self.key_location, "project_id")

        # Use authentication key file to gain access to google cloud project using Oauth2 authentication
        self.authenticate()

        # Dictionary of instances managed by the platform
        self.instances      = {}

        # Google compute zone
        self.zone           = self.config["platform"]["zone"]

        # Standardize formatting of directory paths
        self.format_dirs()

        # Set common directories as attributes so we don't have to type massively long variable names
        self.wrk_dir                = self.config["paths"]["instance_wrk_dir"]
        self.log_dir                = self.config["paths"]["instance_log_dir"]
        self.tmp_dir                = self.config["paths"]["instance_tmp_dir"]
        self.tool_dir               = self.config["paths"]["instance_tool_dir"]
        self.resource_dir           = self.config["paths"]["instance_resource_dir"]
        self.bin_dir                = self.config["paths"]["instance_bin_dir"]
        self.bucket_tool_dir        = self.config["paths"]["bucket_tool_dir"]
        self.bucket_resource_dir    = self.config["paths"]["bucket_resource_dir"]
        self.bucket_output_dir      = self.config["paths"]["bucket_output_dir"]

        # Create clients for using Google PubSub and Google Stackdriver Logging
        self.pubsub         = GooglePubSub()
        self.logging        = GoogleLogging()

        # Name of PubSub topic/subscription for posting instance creation status updates
        self.ready_topic        = None
        self.ready_subscriber   = None

        # Name of PubSub topic/subscription for posting instance preemption status updates
        self.preempt_topic      = None
        self.preempt_subscriber = None

    def clean_up(self):

        logging.info("Cleaning up Google Cloud Platform.")

        if hasattr(self, "instances"):

            # Destroying all the instances
            for instance_name, instance_obj in self.instances.iteritems():
                try:
                    instance_obj.destroy()
                except GoogleException:
                    logging.info("(%s) Could not destroy instance!" % instance_name)

            # Waiting for the instances to be destroyed
            for instance_name, instance_obj in self.instances.iteritems():
                try:
                    instance_obj.wait_process("destroy")
                except GoogleException:
                    logging.info("(%s) Could not destroy instance!" % instance_name)

        if hasattr(self, "ready_subscriber"):
            if self.ready_subscriber is not None:
                self.ready_subscriber.stop()

        if hasattr(self, "preempt_subscriber"):
            if self.preempt_subscriber is not None:
                self.preempt_subscriber.stop()

        if hasattr(self, "pubsub"):
            self.pubsub.clean_up()

        if hasattr(self, "logging"):
            self.logging.clean_up()

        logging.info("Clean up complete!")

    def authenticate(self):

        logging.info("Authenticating to the Google Cloud.")

        if not os.path.exists(self.key_location):
            logging.error("Authentication key was not found!")
            exit(1)

        cmd = "gcloud auth activate-service-account --key-file %s" % self.key_location
        with open(os.devnull, "w") as devnull:
            proc = sp.Popen(cmd, stdout=devnull, stderr=devnull, shell = True)

        if proc.wait() != 0:
            logging.error("Authentication to Google Cloud failed!")
            exit(1)

        logging.info("Authentication to Google Cloud was successful.")

    def prepare_platform(self, sample_data, **kwargs):
        # Takes sample data and initializes a GoogleCompute Platform ready for analysis

        # Pre-launch check to make sure platform configurations are valid
        # A) Check input files, tools, resources, startup/shutdown scripts specified in config exist on bucket
        # B) Check output directory a valid google bucket path
        self.check_platform_before_launch(sample_data)

        # Init pubsub/logging clients for monitoring instances status
        self.prepare_pubsub_logging(sample_data)

        # Init instance and transfer input data, tools, etc.
        self.prepare_main_server(sample_data, **kwargs)

        # Check that all the tools, resources specified in the config actually exist on the main server
        self.check_platform_after_launch()

    def prepare_pubsub_logging(self, sample_data):

        # Generate variables
        ready_topic = "ready_topic_%s" % sample_data["sample_name"]
        ready_sub = "ready_sub_%s" % sample_data["sample_name"]
        preempt_topic = "preempted_topic_%s" % sample_data["sample_name"]
        preempt_sub = "preempted_sub_%s" % sample_data["sample_name"]
        log_sink_name = "preempted_sink_%s" % sample_data["sample_name"]
        log_sink_dest = "pubsub.googleapis.com/projects/%s/topics/%s" % (self.google_project, preempt_topic)
        log_sink_filter = "jsonPayload.event_subtype:compute.instances.preempted"

        # Create topics
        logging.debug("Configuring Google Pub/Sub.")
        self.pubsub.create_topic(ready_topic)
        self.pubsub.create_topic(preempt_topic)
        self.ready_topic   = ready_topic
        self.preempt_topic = preempt_topic

        # Create subscrptions
        self.pubsub.create_subscription(ready_sub, ready_topic)
        self.pubsub.create_subscription(preempt_sub, preempt_topic)

        logging.info("Google Pub/Sub configured.")

        # Create preemption logging sink
        logging.debug("Creating Google Stackdriver Logging sink to Google Pub/Sub.")
        self.logging.create_sink(log_sink_name, log_sink_dest, log_filter=log_sink_filter)
        logging.info("Stackdriver Logging sink to Google Pub/Sub created.")

        #no need to create log-sink for ready topic. Startup-script MUST post directly to
        #the topic upon startup completion otherwise instance will never get registered as 'ready'

        #grant write permission to allow the preempted log sink to write to the preempted PubSub topic
        self.pubsub.grant_write_permission(topic=preempt_topic,
                                           client_json_keyfile=self.key_location,
                                           serv_acct=self.logging.get_serv_acct(log_sink_name))

        # Initialize the subscribers
        logging.debug("Starting the subscribers.")
        self.ready_subscriber = GoogleReadySubscriber(ready_sub, self.instances)
        self.preempt_subscriber = GooglePreemptedSubscriber(preempt_sub, self.instances)

        # Start the subscribers
        self.ready_subscriber.start()
        self.preempt_subscriber.start()

        logging.info("Google Cloud Platform is ready for analysis.")

    def prepare_main_server(self, sample_data, **kwargs):

        # Create main server and wait for it to start
        self.create_main_server(**kwargs)

        # Initialize directory structure on the instance
        self.prepare_env()

        # Copy tools, resources, input data from bucket to instance
        self.prepare_data(sample_data)

        # Memorize the main-server address in the sample data
        sample_data["main-server"] = self.instances["main-server"]

    def prepare_env(self):
        # Create directory structure on main instance

        # Create working directory
        cmd = "mkdir -p %s" % self.wrk_dir
        self.instances["main-server"].run_command("createWrkDir", cmd, proc_wait=True)

        # Create logging directory
        cmd = "mkdir -p %s" % self.log_dir
        self.instances["main-server"].run_command("createLogDir", cmd, proc_wait=True)

        # Create tmp directory
        cmd = "mkdir -p %s" % self.tmp_dir
        self.instances["main-server"].run_command("createTmpDir", cmd, proc_wait=True)

        # Create tool directory
        cmd = "mkdir -p %s" % self.tool_dir
        self.instances["main-server"].run_command("createToolDir", cmd, proc_wait=True)

        # Create resource directory
        cmd = "mkdir -p %s" % self.resource_dir
        self.instances["main-server"].run_command("createTmpDir", cmd, proc_wait=True)

        # Create bin directory
        cmd = "mkdir -p %s" % self.bin_dir
        self.instances["main-server"].run_command("createBinDir", cmd, proc_wait=True)

        # Waiting for all directory creation processes to be done
        self.instances["main-server"].wait_all()

    def prepare_data(self, sample_data):
        # Transfer tools, resources, and input data from bucket to main server

        # Generate options for fast copying
        options_fast = '-m -o "GSUtil:sliced_object_download_max_components=200"'

        # Copy tool folder from bucket if necessary
        if self.bucket_tool_dir is not None:

            cmd = "gsutil %s cp -r %s* %s !LOG3!" % (options_fast, self.bucket_tool_dir, self.tool_dir)
            self.instances["main-server"].run_command("copyTools", cmd)

            #update tools paths to reflect their location on the main server
            self.config["paths"]["tools"] = self.update_paths(self.config["paths"]["tools"],
                                                              source_dir=self.bucket_tool_dir,
                                                              dest_dir=self.tool_dir)

        # Make symbolic links in the bin directory for all exectuables
        for tool_type, tool_path in self.config["paths"]["tools"].iteritems():
            basename = tool_path.split("/")[-1]
            link_name = os.path.join(self.bin_dir, basename)
            cmd = "ln -s %s %s" % (tool_path, link_name)
            self.instances["main-server"].run_command("softlink_%s" % tool_type, cmd)

        # Copy resource folder from bucket if necessary
        if self.bucket_resource_dir is not None:

            cmd = "gsutil %s cp -r %s* %s !LOG3!" % (options_fast, self.bucket_resource_dir, self.resource_dir)
            self.instances["main-server"].run_command("copyResources", cmd)

            #update resources paths to reflect their location on the main server
            self.config["paths"]["resources"] = self.update_paths(self.config["paths"]["resources"],
                                                                  source_dir=self.bucket_resource_dir,
                                                                  dest_dir=self.resource_dir)
        # Copy input files to main server and update paths to reflect their location on main server
        sample_data["input"] = dict()
        for (file_type, gs_file_path) in sample_data["gs_input"].iteritems():
            # Update new filename
            filename = gs_file_path.split("/")[-1]
            local_file_path = "%s%s" % (self.wrk_dir, filename)

            # Registering local path for later use
            sample_data["input"][file_type] = local_file_path

            # Copy to main server
            cmd = "gsutil %s cp %s %s !LOG3!" % (options_fast, gs_file_path, self.wrk_dir)
            self.instances["main-server"].run_command("copyInput_%s" % file_type, cmd)

        # Waiting for all the copying processes to be done
        self.instances["main-server"].wait_all()

        #change permissions on everything to read/write/execute access after everything has been copied
        cmd = "sudo chmod -R 777 %s !LOG3!" % self.wrk_dir
        self.instances["main-server"].run_command("changeDirPermissions", cmd, proc_wait=True)

    def create_main_server(self, **kwargs):

        # Generating arguments dictionary
        kwargs["nr_cpus"]           = kwargs.get("nr_cpus",             self.config["platform"]["MS_nr_cpus"])
        kwargs["mem"]               = kwargs.get("mem",                 self.config["platform"]["MS_mem"])
        kwargs["nr_local_ssd"]      = kwargs.get("nr_local_ssd",        self.config["platform"]["MS_local_ssds"])
        kwargs["boot_disk_size"]    = kwargs.get("boot_disk_size",      self.config["platform"]["boot_disk_size"])
        kwargs["disk_image"]        = kwargs.get("disk_image",          self.config["platform"]["disk_image"])
        kwargs["start_up_script"]   = kwargs.get("start_up_script",     self.config["platform"]["start_up_script"])
        kwargs["shutdown_script"]   = kwargs.get("shutdown_script",     self.config["platform"]["shutdown_script"])
        kwargs["is_boot_disk_ssd"]  = kwargs.get("is_boot_disk_ssd",    self.config["platform"]["is_boot_disk_ssd"])
        kwargs["ready_topic"]       = kwargs.get("ready_topic",         self.ready_topic)
        kwargs["instances"]         = kwargs.get("instances",           self.instances)
        kwargs["service_acct"]      = kwargs.get("service_acct",        self.service_acct)
        kwargs["zone"]              = kwargs.get("zone",                self.zone)
        kwargs["instance_log_dir"]  = kwargs.get("instance_log_dir",    self.log_dir)
        kwargs["is_preemptible"]    = kwargs.get("is_preemptible",      False)
        kwargs["is_server"]         = kwargs.get("is_server",           True)

        # Create the main server
        self.instances["main-server"] = Instance(self.config, "main-server", **kwargs)
        self.instances["main-server"].create()
        self.instances["main-server"].wait_process("create")

    def create_split_server(self, server_name, **kwargs):

        # Updating the kwargs
        kwargs["nr_cpus"]           = kwargs.get("nr_cpus",         None)
        kwargs["mem"]               = kwargs.get("mem",             None)

        if kwargs["nr_cpus"] is None:
            logging.error("(%s) Cannot create instance, because the required number of vCPUs has not been specified!" % server_name)
            raise GoogleException(server_name)

        if kwargs["mem"] is None:
            logging.error("(%s) Cannot create instance, because the required amount of memory has not been specified!" % server_name)
            raise GoogleException(server_name)

        kwargs["boot_disk_size"]    = kwargs.get("boot_disk_size",      self.config["platform"]["split_boot_disk_size"])
        kwargs["disk_image"]        = kwargs.get("disk_image",          self.config["platform"]["split_disk_image"])
        kwargs["start_up_script"]   = kwargs.get("start_up_script",     self.config["platform"]["start_up_script"])
        kwargs["shutdown_script"]   = kwargs.get("shutdown_script",     self.config["platform"]["shutdown_script"])
        kwargs["is_preemptible"]    = kwargs.get("is_preemptible",      self.config["platform"]["is_split_preemptible"])
        kwargs["is_boot_disk_ssd"]  = kwargs.get("is_boot_disk_ssd",    self.config["platform"]["is_boot_disk_ssd"])
        kwargs["ready_topic"]       = kwargs.get("ready_topic",         self.ready_topic)
        kwargs["instances"]         = kwargs.get("instances",           self.instances)
        kwargs["service_acct"]      = kwargs.get("service_acct",        self.service_acct)
        kwargs["zone"]              = kwargs.get("zone",                self.zone)
        kwargs["instance_log_dir"]  = kwargs.get("instance_log_dir",    self.log_dir)
        kwargs["main_server"]       = kwargs.get("main_server",         "main-server")
        kwargs["nr_local_ssd"]      = kwargs.get("nr_local_ssd",        0)
        kwargs["is_server"]         = kwargs.get("is_server",           False)

        # Creating the split servers
        self.instances[server_name] = Instance(self.config, server_name, **kwargs)
        self.instances[server_name].create()

    def finalize(self, sample_data, only_logs=False):

        # Generate destination prefix
        dest_dir = os.path.join(self.bucket_output_dir, sample_data["sample_name"])

        # Copy final outputs
        if not only_logs:

            if "final_output" in sample_data:
                for module_name, outputs in sample_data["final_output"].iteritems():
                    if len(outputs) == 0:
                        continue

                    elif len(outputs) == 1:
                        cmd = "gsutil -m cp -r %s %s/ !LOG3!" % (outputs[0], dest_dir)

                    else:
                        cmd = "gsutil -m cp -r $path %s/%s/ !LOG3!" % (dest_dir, module_name)
                        cmd = "for path in %s; do %s & done; wait" % (" ".join(outputs), cmd)

                    self.instances["main-server"].run_command("copyOut_%s" % module_name, cmd)

            # Waiting for all the copying processes to be done
            self.instances["main-server"].wait_all()

        # Copy the logs
        cmd = "gsutil -m cp -r %s %s/ !LOG0!" % (self.log_dir, dest_dir)
        if "main-server" in self.instances:
            self.instances["main-server"].run_command("copyLogs", cmd)
            self.instances["main-server"].wait_process("copyLogs")

    def check_platform_before_launch(self, sample_data):
        # Checks to see whether files specified in the config actually exist and are properly formatted

        # Check to see if disk image exists and is larger than requested boot disk size
        self.check_disk_image(self.config["platform"]["disk_image"], self.config["platform"]["boot_disk_size"], "Main-Server")

        # Check to see if split disk image exists and is larger than requested split boot disk
        self.check_disk_image(self.config["platform"]["split_disk_image"], self.config["platform"]["split_boot_disk_size"], "Split-Server")

        # Check startup script
        if self.config["platform"]["start_up_script"] is not None:
            self.check_bucket_file_exists(self.config["platform"]["start_up_script"], "Startup script")

        # Check shutdown script
        if self.config["platform"]["shutdown_script"] is not None:
            self.check_bucket_file_exists(self.config["platform"]["shutdown_script"], "Shutdown script")

        # Check tool directory and that all tools on the cloud actually appear in the directory
        if self.bucket_tool_dir is not None:
            self.check_bucket_file_exists(self.bucket_tool_dir, "Tool directory")
            self.check_bucket_files_exist(self.config["paths"]["tools"], path_type="Tools", target_bucket=self.bucket_tool_dir)

        # Check resource directory and that all resources on the cloud actually appear in the directory
        if self.bucket_resource_dir is not None:
            self.check_bucket_file_exists(self.bucket_resource_dir, "Resources directory")
            self.check_bucket_files_exist(self.config["paths"]["resources"], path_type="Resources", target_bucket=self.bucket_resource_dir)

        # Check to make sure input files exist on bucket
        if len(sample_data["gs_input"]) == 0:
            logging.error("No input has been provided to the pipeline!")
            raise IOError("No input has been provided to the pipeline!")
        self.check_bucket_files_exist(sample_data["gs_input"], path_type="Sample Input")

        # Check to make sure output directory is a properly formatted google bucket path
        self.check_output_dir()

    def check_platform_after_launch(self):
        # Checks to see whether resource/tool files specified in the config actually exist on the instance

        # Check tools files to see whether they exist on main server
        for file_type, file_path in self.config["paths"]["tools"].iteritems():
            # Check to see if bucket file actually exists
            self.check_instance_file_exists(file_path, file_type)

        # Check resources files to see whether they exist on main server
        for file_type, file_path in self.config["paths"]["resources"].iteritems():
            # Check to see if bucket file actually exists
            self.check_instance_file_exists(file_path, file_type)
            # Remove wildcard character.
            # This will only be the case for index files which share a basename that needs to be passed to a specific tool.
            # As an example, Bowtie2 or BWA accepts an index basename instead of the complete name of all index files needed to run the tool
            self.config["paths"]["resources"][file_type] = file_path.rstrip("*")

        # Wait for all processes to finish
        self.instances["main-server"].wait_all()

    def check_bucket_files_exist(self, file_dict, path_type, target_bucket=None):
        # Checks to see whether all files in file_dict exist on the bucket
        # Optionally check whether all files are in the same bucket (default: None)
        # Ignores files that begin with '/' as it assumes these files will be present on the the instance
        # Throws error if either condition is not satisfied and the group name variable is used in the error message
        for file_type, gs_file_path in file_dict.iteritems():

            if target_bucket is None:
                self.check_bucket_file_exists(gs_file_path, file_type)

            else:
                # add directory to filepath if relative path given
                if not (self.is_google_bucket_path(gs_file_path) or gs_file_path.startswith("/")):
                    gs_file_path = os.path.join(target_bucket, gs_file_path)

                # Check to see if tool path is on google bucket
                if self.is_google_bucket_path(gs_file_path):

                    # Check to see if bucket file actually exists
                    self.check_bucket_file_exists(gs_file_path, file_type)

                    # Check to see that tool is actually found in the tool bucket specified
                    if not gs_file_path.startswith(target_bucket):
                        # Raise exception if file exists but isn't found in the tools bucket specified in the config
                        logging.error(
                            "'%s' is present on the bucket (%s) but is not located in the %s bucket specified in the config (%s). Try copying the file(s) to the %s bucket."
                            % (file_type, gs_file_path, path_type, target_bucket, path_type))
                        raise IOError(
                            "Tool '%s' is present on the bucket but is not located in the tools bucket specified in the config. Please check the error messages above!"
                            % file_type)

    def check_bucket_file_exists(self, filename, filetype):
        # Checks to see whether a file exists on the google storage. Throws error otherwise specifying the type of bucket that doesn't exist
        logging.info("Checking existence of '%s' on bucket." % filetype)

        cmd = "gsutil ls %s" %filename
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
        out, err = proc.communicate()

        if len(err) != 0:
            logging.error("%s could not be located on bucket: %s. Please provide a valid filepath in the config file." % (filetype, filename))
            raise IOError("%s could not be located on bucket: %s. Please check the error messages above!" % (filetype, filename))

    def check_instance_file_exists(self, filename, filetype):
        # Checks to see whether file exists on the main server. Throws and error otherwise specifying the type of file that doesn't exist
        logging.info("Checking existence of %s on main instance." % filetype)

        #run ls command to see if file exists
        cmd = "ls %s" % filename
        self.instances["main-server"].run_command("checkExists_%s" % filetype, cmd)
        out, err = self.instances["main-server"].get_proc_output("checkExists_%s" % filetype)

        if len(err) != 0:
            logging.error("'%s' was not found on the instance: %s!" % (filetype, filename))
            raise IOError("%s was not found on the instance.")

    def check_output_dir(self):

        # Check to make sure output bucket for final output is a valid google bucket address
        logging.info("Checking pipeline output directory.")

        if self.bucket_output_dir is None:
            logging.error("No bucket output directory specified in the config! Please specify one.")
            raise IOError("No bucket output directory specified int he config! Please specify one.")

        # Check to make sure string starts with gs://
        if not self.is_google_bucket_path(self.bucket_output_dir):
            logging.error("Output directory specified in config does not begin with 'gs://': %s" % self.bucket_output_dir)
            raise IOError("The output directory provided is not a valid google bucket directory string. Please check the error messages above!")

    def check_disk_image(self, disk_image_name, boot_disk_size, disk_image_type):
        # Checks to make sure that disk image exists and is smaller than a given boot disk size

        # Get disk image info
        disk_image_info = self.get_disk_image_info(disk_image_name, disk_image_type)

        # Get size of disk image
        disk_image_size = int(disk_image_info["diskSizeGb"])

        # Resize boot disk if it's smaller than image size
        if boot_disk_size < disk_image_size:
            logging.error("%s disk image (%s) is larger (%dGB) than the requested size of the boot disk (%dGB). Increase size of %s boot disk in config!"
                          %(disk_image_type, disk_image_name, boot_disk_size, disk_image_size, disk_image_type))
            raise IOError("%s disk image (%s) is larger (%dGB) than the requested size of the boot disk (%dGB). Increase size of %s boot disk in config!"
                          %(disk_image_type, disk_image_name, boot_disk_size, disk_image_size, disk_image_type))

    def update_paths(self, file_dict, source_dir, dest_dir):
        # Takes a dict <file_type, file_path> as an argument and returns the updated name for each element assuming it is being
        # Transferred from the source to the dest directory
        updated_paths = dict()

        for file_type, file_path in file_dict.iteritems():

            # Add source_prefix to filepath if relative path given
            if not (self.is_google_bucket_path(file_path) or file_path.startswith("/")):
                file_path = os.path.join(source_dir, file_path)

            # Update tool path if transferred from bucket to instance
            if file_path.startswith(source_dir):
                updated_paths[file_type] = file_path.replace(source_dir, dest_dir)
            else:
                updated_paths[file_type] = file_path

        return updated_paths

    def format_dirs(self):
        # Standardize formatting of directories specified in config
        for path in self.config["paths"]:
            # Check to make sure path is a string and not a hash (i.e. the tool/resource sublists)
            if isinstance(self.config["paths"][path], basestring) and (path != "ref"):
                # Check to make sure the option hasn't been set to an empty string
                if self.config["paths"][path] is not None:
                    self.config["paths"][path] = self.format_dir(self.config["paths"][path])

    def format_dir(self, dir):
        # Takes a directory path as a parameter and returns standard-formatted directory string '/this/is/my/dir/'
        return dir.rstrip("/") + "/"

    def get_info_from_key_file(self, key_file, info_header):
        # Parse JSON service account key file and return email address associated with account
        logging.info("Extracting %s from JSON key file." % info_header)

        if not os.path.exists(key_file):
            logging.error("Authentication key was not found!")
            exit(1)

        # Parse json into dictionary
        with open(key_file) as kf:
            key_data = json.load(kf)

        # Check to make sure correct key is present in dictionary
        if info_header not in key_data:
            logging.error(
                "'%s' field missing from authentication key file: %s. Check to make sure key exists in file or that file is valid google key file!" % (info_header, key_file))
            exit(1)

        return key_data[info_header]

    def get_disk_image_info(self, disk_image_name, disk_image_type):
        # Returns information about a disk image for a project. Throws error if it doesn't exist.
        logging.info("Checking existence of %s disk image: %s." % (disk_image_type, disk_image_name))

        cmd = "gcloud compute images list --format=json"
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
        out, err = proc.communicate()

        # Load results into json
        disk_images = json.loads(out.rstrip())
        for disk_image in disk_images:
            if disk_image["name"] == disk_image_name:
                return disk_image

        # Throw error if disk image can't be found in project
        logging.error("Unable to find %s disk image: %s" % (disk_image_type, disk_image_name))
        raise IOError("Unable to find disk image %s. Please check error messages above for details!" % disk_image_name)

    def is_google_bucket_path(self, filename):
        # Returns true if file path conforms to Google Bucket style. False otherwise.
        return filename[0:5] == "gs://"

