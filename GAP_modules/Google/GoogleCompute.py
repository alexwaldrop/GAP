import os
import subprocess as sp
import logging

from GAP_modules.Google import GoogleException
from GAP_modules.Google import Instance
from GAP_modules.Google import GoogleReadySubscriber
from GAP_modules.Google import GooglePreemptedSubscriber
from GAP_modules.Google import GooglePubSub
from GAP_modules.Google import GoogleLogging

class GoogleCompute(object):

    def __init__(self, config):
        self.config         = config

        self.key_location   = "keys/Davelab_GAP_key.json"
        self.authenticate()

        self.instances      = {}

        self.zone           = self.config["platform"]["zone"]

        self.pubsub         = GooglePubSub()
        self.logging        = GoogleLogging()

        self.ready_topic    = None

        self.ready_subscriber   = None
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

    def check_input(self, sample_data):

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

    def prepare_platform(self, sample_data):

        # Generate variables
        ready_topic = "ready_topic_%s" % sample_data["sample_name"]
        ready_sub = "ready_sub_%s" % sample_data["sample_name"]
        preempt_topic = "preempted_topic_%s" % sample_data["sample_name"]
        preempt_sub = "preempted_sub_%s" % sample_data["sample_name"]
        log_sink_name = "preempted_sink_%s" % sample_data["sample_name"]
        log_sink_dest = "pubsub.googleapis.com/projects/davelab-gcloud/topics/%s" % preempt_topic
        log_sink_filter = "jsonPayload.event_subtype:compute.instances.preempted"

        # Create topics
        logging.debug("Configuring Google Pub/Sub.")
        self.pubsub.create_topic(ready_topic)
        self.pubsub.create_topic(preempt_topic)
        self.ready_topic = ready_topic

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
        kwargs["nr_cpus"]           = kwargs.get("nr_cpus",         self.config["platform"]["MS_nr_cpus"])
        kwargs["mem"]               = kwargs.get("mem",             self.config["platform"]["MS_mem"])
        kwargs["nr_local_ssd"]      = kwargs.get("nr_local_ssd",    self.config["platform"]["MS_local_ssds"])
        kwargs["is_preemptible"]    = False
        kwargs["is_server"]         = True
        kwargs["ready_topic"]       = self.ready_topic
        kwargs["instances"]         = self.instances

        # Create the main server
        self.instances["main-server"] = Instance(self.config, "main-server", **kwargs)
        self.instances["main-server"].create()
        self.instances["main-server"].wait_process("create")

        # Generate options for fast copying
        options_fast = '-m -o "GSUtil:sliced_object_download_max_components=200"'

        # Create logging directory
        cmd = "mkdir -p /data/logs/"
        self.instances["main-server"].run_command("createLogDir", cmd, proc_wait=True)

        # Copy and configure tools binaries
        cmd = "gsutil %s cp -r gs://davelab_data/tools /data/ !LOG3! ; bash /data/tools/setup.sh" % options_fast
        self.instances["main-server"].run_command("copyTools", cmd)

        # Copy the input files
        sample_data["input"] = dict()
        for (file_type, gs_file_path) in sample_data["gs_input"].iteritems():

            # Generating local path
            filename = gs_file_path.split("/")[-1]
            local_file_path = "/data/%s" % filename

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

        kwargs["nr_local_ssd"]      = kwargs.get("nr_local_ssd",    0)
        kwargs["is_preemptible"]    = kwargs.get("is_preemptible",  True)
        kwargs["is_server"]         = False
        kwargs["ready_topic"]       = self.ready_topic
        kwargs["instances"]         = self.instances
        kwargs["main_server"]       = "main-server"

        # Creating the split servers
        self.instances[server_name] = Instance(self.config, server_name, **kwargs)
        self.instances[server_name].create()

    def finalize(self, sample_data, only_logs=False):

        # Generate destination prefix
        dest_dir = "gs://davelab_temp/outputs/%s" % sample_data["sample_name"]

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
        cmd = "gsutil -m cp -r /data/logs %s/ !LOG0!" % dest_dir
        if "main-server" in self.instances:
            self.instances["main-server"].run_command("copyLogs", cmd)
            self.instances["main-server"].wait_process("copyLogs")
