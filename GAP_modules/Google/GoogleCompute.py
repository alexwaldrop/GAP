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

        # Use authentication key file to gain acces to google cloud project using Oauth2 authentication
        self.authenticate()

        # Dictionary of instances managed by the platform
        self.instances      = {}

        # Google compute zone
        self.zone           = self.config["platform"]["zone"]

        # Standardize formatting of directories specified in config
        self.config["general"]["instance_wrk_dir"]  = self.format_dir(self.config["general"]["instance_wrk_dir"])
        self.config["general"]["instance_log_dir"]  = self.format_dir(self.config["general"]["instance_log_dir"])
        self.config["general"]["instance_tmp_dir"]  = self.format_dir(self.config["general"]["instance_tmp_dir"])
        self.config["general"]["bucket_output_dir"] = self.format_dir(self.config["general"]["bucket_output_dir"])

        # Set common directories as attributes so we don't have to type massively long variable names
        self.wrk_dir            = self.config["general"]["instance_wrk_dir"]
        self.log_dir            = self.config["general"]["instance_log_dir"]
        self.tmp_dir            = self.config["general"]["instance_tmp_dir"]
        self.bucket_output_dir  = self.config["general"]["bucket_output_dir"]

        # Check to make sure bucket output directory is a valid google bucket directory
        self.check_output_dir()

        # Check to make sure startup/shutdown scripts exist on bucket
        self.check_startup_shutdown_scripts()

        # Create clients for using Google PubSub and Google Stackdriver Logging
        self.pubsub         = GooglePubSub()
        self.logging        = GoogleLogging()

        # Name of PubSub topic/subscription for posting instance creation status updates
        self.ready_topic        = None
        self.ready_subscriber   = None

        # Name of PubSub topic/subsription for posting instance preemption status updates
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

    def prepare_platform(self, sample_data):

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

    def prepare_data(self, sample_data, **kwargs):

        # Generating arguments dictionary
        kwargs["nr_cpus"]           = kwargs.get("nr_cpus",             self.config["platform"]["MS_nr_cpus"])
        kwargs["mem"]               = kwargs.get("mem",                 self.config["platform"]["MS_mem"])
        kwargs["nr_local_ssd"]      = kwargs.get("nr_local_ssd",        self.config["platform"]["MS_local_ssds"])
        kwargs["is_preemptible"]    = kwargs.get("is_preemptible",      False)
        kwargs["is_server"]         = kwargs.get("is_server",           True)
        kwargs["ready_topic"]       = kwargs.get("ready_topic",         self.ready_topic)
        kwargs["instances"]         = kwargs.get("instances",           self.instances)
        kwargs["service_acct"]      = kwargs.get("service_acct",        self.service_acct)
        kwargs["is_boot_disk_ssd"]  = kwargs.get("is_boot_disk_ssd",    self.config["platform"]["is_boot_disk_ssd"])
        kwargs["zone"]              = kwargs.get("zone",                self.zone)
        kwargs["boot_disk_size"]    = kwargs.get("boot_disk_size",      self.config["platform"]["boot_disk_size"])
        kwargs["disk_image"]        = kwargs.get("disk_image",          self.config["platform"]["disk_image"])
        kwargs["start_up_script"]   = kwargs.get("start_up_script",     self.config["platform"]["start_up_script"])
        kwargs["shutdown_script"]   = kwargs.get("shutdown_script",     self.config["platform"]["shutdown_script"])
        kwargs["instance_log_dir"]  = kwargs.get("instance_log_dir",    self.log_dir)


        # Create the main server
        self.instances["main-server"] = Instance(self.config, "main-server", **kwargs)
        self.instances["main-server"].create()
        self.instances["main-server"].wait_process("create")

        # Generate options for fast copying
        options_fast = '-m -o "GSUtil:sliced_object_download_max_components=200"'

        # Create tmp directory
        cmd = "mkdir -p %s" % self.tmp_dir
        self.instances["main-server"].run_command("createTmpDir", cmd, proc_wait=True)

        # Create logging directory
        cmd = "mkdir -p %s" % self.log_dir
        self.instances["main-server"].run_command("createLogDir", cmd, proc_wait=True)

        # Copy and configure tools binaries
        cmd = "gsutil %s cp -r gs://davelab_data/tools %s !LOG3! ; bash %stools/setup.sh" % (options_fast, self.wrk_dir, self.wrk_dir)
        self.instances["main-server"].run_command("copyTools", cmd)

        # Copy the input files
        sample_data["input"] = dict()
        for (file_type, gs_file_path) in sample_data["gs_input"].iteritems():

            # Generating local path
            filename = gs_file_path.split("/")[-1]
            local_file_path = "%s%s" % (self.wrk_dir, filename)

            # Registering local path for later use
            sample_data["input"][file_type] = local_file_path

            # Copying the file to the main server
            cmd = "gsutil %s cp %s %s !LOG3!" % (options_fast, gs_file_path, local_file_path)
            self.instances["main-server"].run_command("copyInput_%s" % file_type, cmd)

        # Waiting for all the copying processes to be done
        self.instances["main-server"].wait_all()

        # Memorize the main-server address in the sample data
        sample_data["main-server"] = self.instances["main-server"]

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

        kwargs["nr_local_ssd"]      = kwargs.get("nr_local_ssd",        0)
        kwargs["is_server"]         = kwargs.get("is_server",           False)
        kwargs["ready_topic"]       = kwargs.get("ready_topic",         self.ready_topic)
        kwargs["instances"]         = kwargs.get("instances",           self.instances)
        kwargs["main_server"]       = kwargs.get("main_server",         "main-server")
        kwargs["service_acct"]      = kwargs.get("service_acct",        self.service_acct)
        kwargs["is_boot_disk_ssd"]  = kwargs.get("is_boot_disk_ssd",    self.config["platform"]["is_boot_disk_ssd"])
        kwargs["zone"]              = kwargs.get("zone",                self.zone)
        kwargs["boot_disk_size"]    = kwargs.get("boot_disk_size",      self.config["platform"]["boot_disk_size"])
        kwargs["disk_image"]        = kwargs.get("disk_image",          self.config["platform"]["disk_image"])
        kwargs["start_up_script"]   = kwargs.get("start_up_script",     self.config["platform"]["start_up_script"])
        kwargs["shutdown_script"]   = kwargs.get("shutdown_script",     self.config["platform"]["shutdown_script"])
        kwargs["is_preemptible"]    = kwargs.get("is_preemptible",      self.config["platform"]["is_split_preemptible"])
        kwargs["instance_log_dir"]  = kwargs.get("instance_log_dir",    self.log_dir)

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

    def check_input(self, sample_data):
        # Check to see if input data exists on google bucket
        if len(sample_data["gs_input"]) == 0:
            logging.error("No input has been provided to the pipeline!")
            raise IOError("No input has been provided to the pipeline!")

        logging.info("Checking pipeline I/O.")

        has_errors = False

        for file_type, gs_file_path in sample_data["gs_input"].iteritems():
            cmd = "gsutil ls %s" % gs_file_path
            proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
            out, err = proc.communicate()

            if len(err) != 0:
                has_errors = True
                logging.error("Input file %s could not be accessed. The following error appeared: %s!" % (file_type, err))

        if has_errors:
            raise IOError("The input provided to the pipeline has multiple errors. Please check the error messages above!")

    def check_startup_shutdown_scripts(self):

        # Checks to see if startup/shutdown scripts exists on google bucket
        logging.info("Checking existence of startup/shutdown scripts.")

        startup_script  = self.config["platform"]["start_up_script"]
        shutdown_script = self.config["platform"]["shutdown_script"]

        if startup_script is not None:
            if not self.bucket_file_exists(startup_script):
                logging.error("Start-up script not found on Google Bucket system: %s" % startup_script)
                raise IOError("Startup script could not be located on google bucket system. Please check the error messages above!")

        if shutdown_script is not None:
            if not self.bucket_file_exists(shutdown_script):
                logging.error("Shutdown script not found on Google Bucket system: %s" % shutdown_script)
                raise IOError("Shutdown script could not be located on google bucket system. Please check the error messages above!")

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

    def format_dir(self, dir):
        # Takes a directory path as a parameter and returns standard-formatted directory string '/this/is/my/dir/'
        return dir.rstrip("/") + "/"

    def bucket_file_exists(self, bucket_file):
        # Returns true if bucket file exists, false otherwise

        cmd = "gsutil ls %s" % bucket_file
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
        out, err = proc.communicate()

        if len(err) != 0:
            return False
        return True

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

