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
from GAP_interfaces.Platform import Platform

class GooglePlatform(Platform):

    def __init__(self, config, pipeline_data):

        # Call super constructor
        super(GooglePlatform, self).__init__(config, pipeline_data)

        # Get google access fields from JSON file
        self.key_location   = self.config["platform"]["service_account_key_file"]
        self.service_acct   = self.get_info_from_key_file(self.key_location, "client_email")
        self.google_project = self.get_info_from_key_file(self.key_location, "project_id")

        # Use authentication key file to gain access to google cloud project using Oauth2 authentication
        self.authenticate()

        # Get Google compute zone from config
        self.zone           = self.config["platform"]["zone"]


        # Create clients for using Google PubSub and Google Stackdriver Logging
        self.pubsub         = GooglePubSub()
        self.logging        = GoogleLogging()

        # Name of PubSub topic/subscription for posting instance creation status updates
        self.ready_topic        = None
        self.ready_subscriber   = None

        # Name of PubSub topic/subscription for posting instance preemption status updates
        self.preempt_topic      = None
        self.preempt_subscriber = None

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

    def launch_platform(self, **kwargs):

        # Check to see if disk image exists and is larger than requested boot disk size
        self.validate_disk_image("Main Instance",
                                 disk_image_name=self.config["platform"]["disk_image"],
                                 boot_disk_size=self.config["platform"]["boot_disk_size"])

        # Check to see if split disk image exists and is larger than requested split boot disk

        self.validate_disk_image("Split Instance",
                                 disk_image_name=self.config["platform"]["split_disk_image"],
                                 boot_disk_size=self.config["platform"]["split_boot_disk_size"])

        # Check startup script
        if self.config["platform"]["start_up_script"] is not None:
            self.validate_cloud_storage_file("Startup Script", self.config["platform"]["start_up_script"])

        # Check shutdown script
        if self.config["platform"]["shutdown_script"] is not None:
            self.validate_cloud_storage_file("Shutdown Script", self.config["platform"]["start_up_script"])

        # Launch pubsub/logging for keeping track of instance creation/preemption on platform
        self.launch_pubsub_logging()

        super(GooglePlatform, self).launch_platform()

    def startup_tasks(self, instance, is_main_instance=False):

        # Obtain the list of packages the user requests to be installed
        packages = self.config["platform"]["startup_packages"]

        # Add necessary packages by the platform
        if "nfs-common" not in packages:
            packages.append("nfs-common")

        # Install the packges on the instance
        instance.install_packages(packages, wait=True)

        # Configure CRCMOD
        instance.configure_CRCMOD()

        # Configure SSH
        instance.configure_SSH()

        # Add BIN directory to path
        instance.export_env_variables("PATH", self.bin_dir)

        # Depending on the type of the instance, do the following
        if is_main_instance:

            # Configure RAID system
            instance.configure_RAID(self.wrk_dir)

            # Configure NFS system
            instance.configure_NFS(self.wrk_dir)

        else:

            # Mount the main server
            instance.mount_main_server(self.wrk_dir)

    def launch_pubsub_logging(self):

        # Generate variables
        pipeline_name   = self.pipeline_data.pipeline_name
        ready_topic     = "ready_topic_%s"      % pipeline_name
        ready_sub       = "ready_sub_%s"        % pipeline_name
        preempt_topic   = "preempted_topic_%s"  % pipeline_name
        preempt_sub     = "preempted_sub_%s"    % pipeline_name
        log_sink_name   = "preempted_sink_%s"   % pipeline_name
        log_sink_dest   = "pubsub.googleapis.com/projects/%s/topics/%s" % (self.google_project, preempt_topic)
        log_sink_filter = "jsonPayload.event_subtype:compute.instances.preempted"

        # Create topics
        logging.debug("Configuring Google Pub/Sub.")
        self.pubsub.create_topic(ready_topic)
        self.pubsub.create_topic(preempt_topic)
        self.ready_topic   = ready_topic
        self.preempt_topic = preempt_topic

        # Create subscriptions
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

    def generate_main_instance_name(self):
        # Automated function to generate the name of the main instance
        main_instance_name = "main-instance-%s" % self.pipeline_data.pipeline_id

        # Format to ensure name conforms to google cloud naming conventions
        return self.format_instance_name(main_instance_name)

    def generate_split_instance_name(self, tool_id, module_name, split_id):
        # Automated function to generate a unique name for a split instance
        split_instance_name = "%s-%s-%s-split%d-instance" % (module_name,
                                                             tool_id,
                                                             self.pipeline_data.pipeline_id,
                                                             split_id)

        # Format to ensure name conforms to google cloud naming conventions
        return self.format_instance_name(split_instance_name)

    def init_main_instance(self, instance_name, **kwargs):
        # Returns the main instance object for running a pipeline
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

        return Instance(self.config, instance_name, **kwargs)

    def init_split_instance(self, instance_name, **kwargs):
        # Returns an instance object that will be used to run a split command

        # Updating the kwargs
        kwargs["nr_cpus"]           = kwargs.get("nr_cpus",         None)
        kwargs["mem"]               = kwargs.get("mem",             None)

        if kwargs["nr_cpus"] is None:
            logging.error("(%s) Cannot create instance, because the required number of vCPUs has not been specified!" % instance_name)
            raise GoogleException(instance_name)

        if kwargs["mem"] is None:
            logging.error("(%s) Cannot create instance, because the required amount of memory has not been specified!" % instance_name)
            raise GoogleException(instance_name)

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

        return Instance(self.config, instance_name, **kwargs)

    def file_exists_on_cloud_storage(self, file_name):
        # Determine whether a file exists on Google Storage
        cmd = "gsutil ls %s" % file_name
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
        out, err = proc.communicate()

        return len(err) == 0

    def validate_cloud_output_directory(self):

        # Make sure an output directory is set for the platform
        if self.cloud_storage_output_dir is None:
            logging.error("No bucket output directory specified in the config! Please specify one.")
            exit(1)

        # Make sure output directory begins with 'gs://'
        if self.cloud_storage_output_dir[0:5] != "gs://":
            logging.error("Cloud output bucket specified in config does not begin with 'gs://': %s"
                          % self.cloud_storage_output_dir)
            exit(1)

        # Make sure base bucket actually exists
        bucket = self.cloud_storage_output_dir.split("/")[0:3]
        bucket = "/".join(bucket)
        if not self.file_exists_on_cloud_storage(bucket):
            logging.error("Cloud storage bucket '%s' doesn't exist. Can't write to output directory: %s."
                          % self.cloud_storage_output_dir)
            exit(1)

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

    def validate_disk_image(self, disk_image_type, disk_image_name, boot_disk_size):
        # Checks to make sure that disk image exists and is smaller than a given boot disk size
        logging.info("Checking existence of %s disk image: %s." % (disk_image_type, disk_image_name))

        # Get disk image info
        disk_image_info = self.get_disk_image_info(disk_image_name)

        # Throw error if not found
        if disk_image_info is None:
            logging.error("Unable to find %s disk image: %s!" % (disk_image_type, disk_image_name))
            exit(1)

        # Get size of disk image
        disk_image_size = int(disk_image_info["diskSizeGb"])

        # Throw error if size of boot disk is smaller than the size of the instance
        if boot_disk_size < disk_image_size:
            logging.error("%s disk image (%s) is larger (%dGB) than the requested size of the boot disk (%dGB). "
                          "Increase size of %s boot disk in config!"
                          %(disk_image_type, disk_image_name, boot_disk_size, disk_image_size, disk_image_type))
            exit(1)

    def get_disk_image_info(self, disk_image_name):
        # Returns information about a disk image for a project. Returns none if no image exists with the name.

        cmd = "gcloud compute images list --format=json"
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
        out, err = proc.communicate()

        # Load results into json
        disk_images = json.loads(out.rstrip())
        for disk_image in disk_images:
            if disk_image["name"] == disk_image_name:
                return disk_image

        return None

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

    @staticmethod
    def format_instance_name(instance_name):
        # Ensures that instance name conforms to google cloud formatting specs
        instance_name.replace("_","-")
        instance_name.replace(".","-")
        instance_name = instance_name.lower()
        return instance_name


