import os
import subprocess as sp
import logging
import socket
import threading

from GoogleException import GoogleException
from Instance import Instance

class GoogleCompute(object):


    class SocketReceiver(threading.Thread):

        def __init__(self, ip, port, instances):
            super(GoogleCompute.SocketReceiver, self).__init__()

            self.server_ip = ip
            self.server_port = port

            self.instances = instances

            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.socket.bind((self.server_ip, self.server_port))

            self.listening = True

            self.daemon = True

        def run(self):

            while self.listening:
                data = self.socket.recv(1024)
                logging.debug("Received the following message: %s" % data)
                self.process_message(data)

        def stop(self):

            self.listening = False

        def process_message(self, msg):

            status, inst_name = msg.split(" ", 1)

            if status == "READY":
                logging.debug("(%s) READY signal received!" % inst_name)
                self.instances[inst_name].set_status(Instance.AVAILABLE)
                logging.info("(%s) Instance ready!" % inst_name)

            elif status == "DEAD":
                logging.debug("(%s) DEAD signal received!" % inst_name)
                self.instances[inst_name].set_status(Instance.DEAD)


    def __init__(self, config):
        self.config         = config

        self.key_location   = "keys/Davelab_GAP_key.json"
        self.authenticate()

        self.instances      = {}
        self.disks          = {}

        self.zone           = self.get_zone()

        self.server_ip      = None
        self.server_port    = None
        self.socket         = None
        self.create_socket()

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

    def create_socket(self):

        self.server_ip = socket.gethostbyname(socket.gethostname())
        self.server_port = 27708

        self.socket = self.SocketReceiver(self.server_ip, self.server_port, self.instances)
        self.socket.start()

    @staticmethod
    def get_zone():

        p = sp.Popen(["gcloud config list 2>/dev/null | grep \"zone\""], stdout = sp.PIPE, stderr = sp.PIPE, shell = True)
        output = p.communicate()[0]

        if len(output) != 0:
            return output.strip().split("=")[-1]
        else:
            logging.info("No zone is specified in the local config file! 'us-east1-b' is selected by default!")
            return "us-east1-b"

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
        self.instances["main-server"].run_command("createLogDir", cmd, log=False, proc_wait=True)

        # Copying input data
        options_fast = '-m -o "GSUtil:sliced_object_download_max_components=200"'
        cmd = "gsutil %s cp %s %s " % (options_fast, sample_data["R1_source"], sample_data["R1"])
        self.instances["main-server"].run_command("copyFASTQ_R1", cmd)

        cmd = "gsutil %s cp %s %s " % (options_fast, sample_data["R2_source"], sample_data["R2"])
        self.instances["main-server"].run_command("copyFASTQ_R2", cmd)

        # Copying and configuring the softwares
        cmd = "gsutil -m cp -r gs://davelab_data/tools /data/ ; bash /data/tools/setup.sh"
        self.instances["main-server"].run_command("copyTools", cmd)

        # Waiting for all the copying processes to be done
        self.instances["main-server"].wait_all()

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
        kwargs["instances"]         = self.instances
        kwargs["main_server"]       = "main-server"

        # Creating the split servers
        self.instances[server_name] = Instance(server_name, nr_cpus, mem, **kwargs)
        self.instances[server_name].create()

    def finalize(self, sample_data, only_logs=False):

        # Exiting if no outputs are present
        if "outputs" not in sample_data:
            return None

        # Copying the logs
        cmd = "gsutil -m cp -r /data/logs gs://davelab_temp/outputs/%s/" % (sample_data["sample_name"])
        self.instances["main-server"].run_command("copyLogs", cmd)

        if only_logs:
            return

        # Copying the output data
        for i, output_path in enumerate(sample_data["outputs"]):
            cmd = "gsutil -m cp -r %s gs://davelab_temp/outputs/%s/" % (output_path, sample_data["sample_name"])
            self.instances["main-server"].run_command("copyOut_%d" % i, cmd)

        # Waiting for all the copying processes to be done
        self.instances["main-server"].wait_all()