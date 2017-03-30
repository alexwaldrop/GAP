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
        self.disks          = {}

        self.zone           = self.get_zone()

        self.pubsub         = GooglePubSub()
        self.logging        = GoogleLogging()

        self.ready_topic    = None

        self.ready_subscriber   = None
        self.preempt_subscriber = None

    def __del__(self):

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

        if hasattr(self, "disks"):

            # Destroying all the disks
            for disk_name, disk_obj in self.disks.iteritems():
                if disk_obj.processes.get("destroy") is None:
                    try:
                        disk_obj.destroy()
                    except GoogleException:
                        logging.info("(%s) Could not destroy disk!" % disk_name)

            # Waiting for the disks to be destroyed
            for disk_name, disk_obj in self.disks.iteritems():
                try:
                    disk_obj.wait_process("destroy")
                except GoogleException:
                    logging.info("(%s) Could not destroy disk!" % disk_name)

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

    @staticmethod
    def get_zone():

        p = sp.Popen(["gcloud config list 2>/dev/null | grep \"zone\""], stdout = sp.PIPE, stderr = sp.PIPE, shell = True)
        output = p.communicate()[0]

        if len(output) != 0:
            return output.strip().split("=")[-1]
        else:
            logging.info("No zone is specified in the local config file! 'us-east1-b' is selected by default!")
            return "us-east1-b"

    def prepare_platform(self, sample_data):

        # Generate variables
        ready_topic = "%s_ready_topic" % sample_data["sample_name"]
        ready_sub = "%s_ready_sub" % sample_data["sample_name"]
        preempt_topic = "%s_preempted_topic" % sample_data["sample_name"]
        preempt_sub = "%s_preempted_sub" % sample_data["sample_name"]
        log_sink_name = "%s_preempted_sink" % sample_data["sample_name"]
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

        # Initialize the subscribers
        logging.debug("Starting the subscribers.")
        self.ready_subscriber = GoogleReadySubscriber(ready_sub, self.instances)
        self.preempt_subscriber = GooglePreemptedSubscriber(preempt_sub, self.instances)

        # Start the subscribers
        self.ready_subscriber.start()
        self.preempt_subscriber.start()

        logging.info("Google Cloud Platform is ready for analysis.")

    def prepare_data(self, sample_data, nr_cpus=None, mem=None, nr_local_ssd=3):

        # Setting the arguments with default values
        if nr_cpus is None:
            nr_cpus = self.config["instance"]["nr_cpus"]
        if mem is None:
            mem = self.config["instance"]["mem"]

        # Obtaining the cpus and memory that will actually be used
        nr_cpus, mem, instance_type = Instance.get_type(nr_cpus, mem)
        self.config["instance"]["nr_cpus"] = nr_cpus
        self.config["instance"]["mem"] = mem

        # Generating arguments dictionary
        kwargs = dict()
        kwargs["instance_type"]     = instance_type
        kwargs["is_preemptible"]     = False
        kwargs["is_server"]         = True
        kwargs["ready_topic"]       = self.ready_topic
        kwargs["nr_local_ssd"]      = nr_local_ssd
        kwargs["instances"]         = self.instances

        # Create the main server
        self.instances["main-server"] = Instance("main-server", nr_cpus, mem, **kwargs)
        self.instances["main-server"].create()
        self.instances["main-server"].wait_process("create")

        # Adding new paths
        sample_data["R1"] = "/data/R1_%s.fastq.gz" % sample_data["sample_name"]
        sample_data["R2"] = "/data/R2_%s.fastq.gz" % sample_data["sample_name"]

        # Creating logging directory
        cmd = "mkdir -p /data/logs/"
        self.instances["main-server"].run_command("createLogDir", cmd, proc_wait=True)

        # Copying input data
        options_fast = '-m -o "GSUtil:sliced_object_download_max_components=200"'
        cmd = "gsutil %s cp %s %s !LOG3!" % (options_fast, sample_data["R1_source"], sample_data["R1"])
        self.instances["main-server"].run_command("copyFASTQ_R1", cmd)

        cmd = "gsutil %s cp %s %s !LOG3!" % (options_fast, sample_data["R2_source"], sample_data["R2"])
        self.instances["main-server"].run_command("copyFASTQ_R2", cmd)

        # Copying and configuring the softwares
        cmd = "gsutil %s cp -r gs://davelab_data/tools /data/ !LOG3! ; bash /data/tools/setup.sh" % (options_fast)
        self.instances["main-server"].run_command("copyTools", cmd)

        # Waiting for all the copying processes to be done
        self.instances["main-server"].wait_all()

        # Memorize the main-server address in the sample data
        sample_data["main-server"] = self.instances["main-server"]

    def create_split_server(self, server_name, nr_cpus=None, mem=None, **kwargs): #nr_cpus=None, mem=None, nr_local_ssd=1, is_preemptible=True):

        # Obtaining the cpus and memory that will actually be used
        if nr_cpus is None:
            nr_cpus = self.config["instance"]["nr_cpus"]
        if mem is None:
            mem = self.config["instance"]["mem"]
        nr_cpus, mem, instance_type = Instance.get_type(nr_cpus, mem)
        self.config["instance"]["nr_cpus"] = nr_cpus
        self.config["instance"]["mem"] = mem

        # Updating the kwargs
        kwargs["instance_type"]     = instance_type
        kwargs["is_preemptible"]    = kwargs.get("is_preemptible", True)
        kwargs["nr_local_ssd"]      = kwargs.get("nr_local_ssd", 0)
        kwargs["is_server"]         = False
        kwargs["ready_topic"]       = self.ready_topic
        kwargs["instances"]         = self.instances
        kwargs["main_server"]       = "main-server"

        # Creating the split servers
        self.instances[server_name] = Instance(server_name, nr_cpus, mem, **kwargs)
        self.instances[server_name].create()

    def finalize(self, sample_data, only_logs=False):

        if not only_logs:

            # Copying the bam, if exists
            if "bam" in sample_data:
                cmd = "gsutil -m cp -r %s gs://davelab_temp/outputs/%s/%s.bam !LOG3!" % (sample_data["bam"], sample_data["sample_name"], sample_data["sample_name"])
                self.instances["main-server"].run_command("copyBAM", cmd)

            # Copying the bam index, if exists
            if "bam_index" in sample_data:
                cmd = "gsutil -m cp -r %s gs://davelab_temp/outputs/%s/%s.bam.bai !LOG3!" % (sample_data["bam_index"], sample_data["sample_name"], sample_data["sample_name"])
                self.instances["main-server"].run_command("copyBAI", cmd)

            # Copying the output data, if exists
            if "outputs" in sample_data:
                for module_name, output_paths in sample_data["outputs"].iteritems():
                    if len(output_paths) == 1:
                        cmd = "gsutil -m cp -r %s gs://davelab_temp/outputs/%s/ !LOG3!" % (output_paths[0], sample_data["sample_name"])
                    else:
                        cmd = "gsutil -m cp -r $path gs://davelab_temp/outputs/%s/%s/ !LOG3!" % (sample_data["sample_name"], module_name)
                        cmd = "for path in %s; do %s & done" % (" ".join(output_paths), cmd)

                    self.instances["main-server"].run_command("copyOut_%s" % module_name, cmd)

            # Waiting for all the copying processes to be done
            self.instances["main-server"].wait_all()

        # Copying the logs
        cmd = "gsutil -m cp -r /data/logs gs://davelab_temp/outputs/%s/" % (sample_data["sample_name"])
        self.instances["main-server"].run_command("copyLogs", cmd)
        self.instances["main-server"].wait_process("copyLogs")
