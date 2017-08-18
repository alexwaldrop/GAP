import logging
import subprocess as sp
import threading
import random
import time
import requests
import math
import json

from System.Platform import Process
from System.Platform import Processor

class GoogleStandardProcessor(Processor):

    # Instance status values available between threads
    OFF         = 0     # Destroyed or not allocated on the cloud
    AVAILABLE   = 1     # Available for running processes
    BUSY        = 2     # Instance actions, such as create and destroy are running
    DEAD        = 3     # Instance is shutting down, as a DEAD signal was received
    MAX_STATUS  = 3     # Maximum status value possible

    def __init__(self, name, nr_cpus, mem, **kwargs):
        # Call super constructor
        super(GoogleStandardProcessor,self).__init__(name, nr_cpus, mem, **kwargs)

        # Get required arguments
        self.zone               = kwargs.pop("zone")
        self.service_acct       = kwargs.pop("service_acct")
        self.boot_disk_size     = kwargs.pop("boot_disk_size")
        self.disk_image         = kwargs.pop("disk_image")

        # Get optional arguments
        self.is_boot_disk_ssd   = kwargs.pop("is_boot_disk_ssd",    False)
        self.nr_local_ssd       = kwargs.pop("nr_local_ssd",        0)

        # Get maximum resource settings
        self.MAX_NR_CPUS        = kwargs.get("PROC_MAX_NR_CPUS",    16)
        self.MAX_MEM            = kwargs.get("PROC_MAX_MEM",        208)

        # Indicates that instance is not resettable
        self.is_preemptible = False

        # Setting the instance status
        self.status_lock = threading.Lock()
        self.status = GoogleStandardProcessor.OFF

    def set_status(self, new_status):
        # Updates instance status with threading.lock() to prevent race conditions
        if new_status > GoogleStandardProcessor.MAX_STATUS or new_status < 0:
            logging.debug("(%s) Status level %d not available!" % (self.name, new_status))
            raise RuntimeError("Instance %s has failed!" % self.name)
        with self.status_lock:
            self.status = new_status

    def get_status(self):
        # Returns instance status with threading.lock() to prevent race conditions
        with self.status_lock:
            return self.status

    def create(self):
        # Begin running command to create the instance on Google Cloud

        # Set status to indicate that commands can't be run on processor because it's busy
        self.set_status(GoogleStandardProcessor.BUSY)

        # Get Google instance type
        instance_type = self.get_instance_type()

        logging.info("(%s) Process 'create' started!" % self.name)
        logging.debug("(%s) Instance type is %s." % (self.name, instance_type))

        # Create base command
        args = list()
        args.append("gcloud compute instances create %s" % self.name)

        # Specify the zone where instance will exits
        args.append("--zone")
        args.append(self.zone)

        # Specify that instance is not preemptible
        if self.is_preemptible:
            args.append("--preemptible")

        # Specify boot disk image
        args.append("--image")
        args.append(str(self.disk_image))

        # Set boot disk size
        args.append("--boot-disk-size")
        if self.boot_disk_size >= 1024:
            args.append("%dTB" % int(self.boot_disk_size/1024))
        else:
            args.append("%dGB" % int(self.boot_disk_size))

        # Set boot disk type
        args.append("--boot-disk-type")
        if self.is_boot_disk_ssd:
            args.append("pd-ssd")
        else:
            args.append("pd-standard")

        # Add local ssds if necessary
        args.extend(["--local-ssd interface=scsi" for _ in xrange(self.nr_local_ssd)])

        # Specify google cloud access scopes
        args.append("--scopes")
        args.append("cloud-platform")

        # Specify google cloud service account
        args.append("--service-account")
        args.append(str(self.service_acct))

        # Determine Google Instance type and insert into gcloud command
        if "custom" in instance_type:
            args.append("--custom-cpu")
            args.append(str(self.nr_cpus))

            args.append("--custom-memory")
            args.append(str(self.mem))
        else:
            args.append("--machine-type")
            args.append(instance_type)

        # Add metadata to run base Google startup-script
        startup_script_location = "System/Platform/Google/GoogleStartupScript.sh"
        args.append("--metadata-from-file")
        args.append("startup-script=%s" % startup_script_location)

        # Run command, wait for instance to appear on Google Cloud
        self.processes["create"] = Process(" ".join(args), stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
        self.wait_process("create")

        # Wait for ssh to initialize and startup script to complete after instance is live
        self.wait_until_ready()
        logging.info("(%s) Instance startup complete! %s is now live and ready to run commands!" % (self.name, self.name))

        # Update status to available and exit
        self.set_status(GoogleStandardProcessor.AVAILABLE)

    def destroy(self, wait=True):
        # Begin running command to destroy instance on Google Cloud

        # Return if instance has already been destroyed
        if self.get_status() == GoogleStandardProcessor.OFF:
            return

        # Set status to indicate that instance cannot run commands and is destroying
        self.set_status(GoogleStandardProcessor.BUSY)

        logging.info("(%s) Process 'destroy' started!" % self.name)

        # Create base command to destroy instance
        args = list()
        args.append("gcloud compute instances delete %s" % self.name)

        # Specify the zone where instance is running
        args.append("--zone")
        args.append(self.zone)

        # Provide input to the command
        args[0:0] = ["yes", "2>/dev/null", "|"]

        # Run command, wait for destroy to complete, and set status to 'OFF'
        self.processes["destroy"] = Process(" ".join(args), stdout=sp.PIPE, stderr=sp.PIPE, shell=True)

        # Wait for delete to complete if requested
        if wait:
            self.wait_process("destroy")

    def wait_process(self, proc_name):
        # Get process from process list
        proc_obj = self.processes[proc_name]

        # Return immediately if process has already been set to complete
        if proc_obj.is_complete():
            return

        # Wait for process to finish
        # Communicate used to prevent stdout and stderr buffers from filling and deadlocking
        out, err = proc_obj.communicate()

        # Set process to complete
        proc_obj.set_complete()

        # Case: Process completed with errors
        if proc_obj.has_failed():
            # Check to see whether error is fatal
            if self.is_fatal_error(proc_name, err):
                logging.error("(%s) Process '%s' failed!" % (self.name, proc_name))
                logging.error("(%s) The following error was received: \n  %s\n%s" % (self.name, out, err))
                raise RuntimeError("Instance %s has failed!" % self.name)

        # Set status to 'OFF' if destroy is True
        if proc_name == "destroy":
            self.set_status(GoogleStandardProcessor.OFF)

        # Case: Process completed
        logging.info("(%s) Process '%s' complete!" % (self.name, proc_name))
        return out, err

    def set_env_variable(self, env_variable, path):
        # Set path to bash script that will export the environment variables
        export_loc  = "/etc/profile.d/pipeline_export.sh"
        # Export the variable
        cmd         = "sudo bash -c 'echo \"%s=%s:\\$%s\" >> %s' " % (env_variable, path, env_variable, export_loc)
        # Get job name
        job_name    = "export_path_%s_%d" % (env_variable, random.randint(1,100000))

        # Run job and wait to finish
        self.run(job_name, cmd)
        self.wait_process(job_name)

    def mount(self, parent_instance_name, parent_mount_point, child_mount_point):
        # Mount another instance at a mount_point

        # Install nfs-common to allow mounting
        self.install_packages("nfs-common")

        # Generate command for mounting main instance
        logging.info("(%s) Mounting to %s." % (self.name, parent_instance_name))
        cmd = "sudo mkdir -p %s && sudo mount -t nfs %s:%s %s !LOG0!" % (child_mount_point,
                                                                         parent_instance_name,
                                                                         parent_mount_point,
                                                                         child_mount_point)
        # Run command and return when complete
        self.run("mountNFS", cmd)
        self.wait_process("mountNFS")

    def configure_CRCMOD(self):
        # Install necessary packages
        self.install_packages(["gcc", "python-dev", "python-setuptools"])

        # Install CRCMOD python package
        logging.info("(%s) Configuring CRCMOD for fast data tranfer using gsutil." % self.name)
        cmd = "sudo easy_install -U pip && sudo pip uninstall -y crcmod && sudo pip install -U crcmod"
        self.run("configCRCMOD", cmd)
        self.wait_process("configCRCMOD")

    def install_packages(self, packages):
        # If no packages are provided to install
        if not packages:
            return

        if not isinstance(packages, list):
            packages = [packages]

        # Log installation
        logging.info("(%s) Installing the following packages: %s" % (self.name, " ".join(packages)))

        # Get command to install packages
        cmd         = "yes | sudo aptdcon --hide-terminal -i \"%s\" !LOG0! " % " ".join(packages)
        # Create random id for job
        job_name    = "install_packages_%d" % random.randint(1,100000)
        self.run(job_name, cmd)
        self.wait_process(job_name)

    def configure_SSH(self, max_connections=500):
        # Increase the number of concurrent SSH connections
        logging.info("(%s) Increasing the number of maximum concurrent SSH connections to %s." % (self.name, max_connections))
        cmd = "sudo bash -c 'echo \"MaxStartups %s\" >> /etc/ssh/sshd_config'" % max_connections
        self.run("configureSSH", cmd)
        self.wait_process("configureSSH")

        # Restart SSH daemon to load the settings
        logging.info("(%s) Restarting SSH daemon to load the new settings." % self.name)
        cmd = "sudo service sshd restart"
        self.run("restartSSH", cmd)
        self.wait_process("restartSSH")

    def adapt_cmd(self, cmd):
        # Adapt command for running on instance through gcloud ssh
        cmd = cmd.replace("'", "'\"'\"'")
        cmd = "gcloud compute ssh gap@%s --command '%s' --zone %s" % (self.name, cmd, self.zone)
        return cmd

    def is_fatal_error(self, proc_name, err_msg):
        # Check to see if program should exit due to error received
        if proc_name == "destroy":
            # Check if 'destroy' process actually deleted the instance, in which case program can continue running
            cmd = 'gcloud compute instances list | grep "%s"' % self.name
            out, _ = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True).communicate()
            if len(out) == 0:
                return False
        return True

    def wait_until_ready(self):
        # Wait until startup-script has completed on instance
        # This signifies that the instance has initialized ssh and the instance environment is finalized

        logging.info("(%s) Waiting for instance startup-script completion..." % self.name)
        ready = False
        cycle_count = 1

        # Waiting 20 minutes for the instance to finish running
        while cycle_count < 600 and not ready:
            # Check the syslog to see if it contains text indicating the startup has completed
            cmd         = 'gcloud compute instances describe %s --format json --zone %s' % (self.name, self.zone)
            proc        = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
            out, err    = proc.communicate()

            # Raise error if unable to get syslog from instance
            if len(err) > 0:
                logging.error("(%s) Unable to poll startup! Received the following error:\n%s" % (self.name, err))
                raise RuntimeError("Instance %s has failed!" % self.name)

            # Check to see if "READY" has been added to instance metadata indicating startup-script has complete
            data = json.loads(out)
            for item in data["metadata"]["items"]:
                if item["key"] == "READY":
                    ready = True

            # Sleep for a couple secs and try all over again if nothing was found
            time.sleep(2)
            cycle_count += 1

        # Raise error if instance not initialized within the alloted timeframe
        if not ready:
            logging.error("(%s) Instance failed! 'Create' Process took more than 20 minutes!" % self.name)
            raise RuntimeError("Instance %s has failed!" % self.name)

    def configure_NFS(self, exported_dir):

        # Install required packages
        self.install_packages(["sysv-rc-conf", "nfs-kernel-server"])

        # Setup the runlevels
        logging.info("(%s) Configuring the runlevels for NFS server." % self.name)
        cmd = "sudo sysv-rc-conf nfs on && sudo sysv-rc-conf rpcbind on"
        self.run("setupNFS", cmd)
        self.wait_process("setupNFS")

        # Export the NFS server
        logging.info("(%s) Exporting the NFS server directory." % self.name)
        cmd = "sudo sh -c \"echo '\n%s\t10.240.0.0/16(rw,sync,no_subtree_check,root_squash,nohide,sec=sys)\n' >> /etc/exports\" " % exported_dir
        self.run("exportNFS", cmd)
        self.wait_process("exportNFS")

        # Restart NFS server
        logging.info("(%s) Restarting NFS server to load the new settings." % self.name)
        cmd = "sudo service nfs-kernel-server restart"
        self.run("restartNFS", cmd)
        self.wait_process("restartNFS")

    def configure_RAID(self, raid_dir):

        # Install the required packages
        self.install_packages("mdadm")

        # Setup the RAID system
        logging.info("(%s) Configuring RAID-0 system by merging the Local SSDs." % self.name)
        cmd = "sudo mdadm --create /dev/md0 --level=0 --raid-devices=%d $(ls /dev/disk/by-id/* | grep google-local-ssd)" % self.nr_local_ssd
        self.run("configRAID", cmd)
        self.wait_process("configRAID")

        # Format the RAID partition
        logging.info("(%s) Formating RAID partition." % self.name)
        cmd = "sudo mkfs -t ext4 /dev/md0"
        self.run("formatRAID", cmd)
        self.wait_process("formatRAID")

        # Mount the RAID partition
        logging.info("(%s) Mounting the RAID partition." % self.name)
        cmd = "sudo mkdir -p %s && sudo mount -t ext4 /dev/md0 %s" % (raid_dir, raid_dir)
        self.run("mountRAID", cmd)
        self.wait_process("mountRAID")

        # Change permission on the the RAID partition
        logging.info("(%s) Changing permissions for the RAID partition." % self.name)
        cmd = "sudo chmod -R 777 %s" % raid_dir
        self.run("chmodRAID", cmd)
        self.wait_process("chmodRAID")

    def get_instance_type(self):

        # Making sure the values are not higher than possibly available
        if self.nr_cpus > self.MAX_NR_CPUS:
            logging.error("(%s) Cannot provision an instance with %d vCPUs. Maximum is %d vCPUs." % (self.name, self.nr_cpus, self.MAX_NR_CPUS))
            raise RuntimeError("Instance %s has failed!" % self.name)
        if self.mem > self.MAX_MEM:
            logging.error("(%s) Cannot provision an instance with %d GB RAM. Maximum is %d GB RAM." % (self.name, self.mem, self.MAX_MEM))
            raise RuntimeError("Instance %s has failed!" % self.name)

        # Obtaining prices from Google Cloud Platform
        try:
            price_json_url = "https://cloudpricingcalculator.appspot.com/static/data/pricelist.json"

            # Disabling low levels of logging from module requests
            logging.getLogger("requests").setLevel(logging.WARNING)

            prices = requests.get(price_json_url).json()["gcp_price_list"]
        except BaseException as e:
            if e.message != "":
                logging.error("Could not obtain instance prices. The following error appeared: %s." % e.message)
            raise

        # Defining instance types to mem/cpu ratios
        ratio = dict()
        ratio["highcpu"] = 1.80 / 2
        ratio["standard"] = 7.50 / 2
        ratio["highmem"] = 13.00 / 2

        # Identifying needed predefined instance type
        if self.nr_cpus == 1:
            instance_type = "standard"
        else:
            ratio_mem_cpu = self.mem * 1.0 / self.nr_cpus
            if ratio_mem_cpu <= ratio["highcpu"]:
                instance_type = "highcpu"
            elif ratio_mem_cpu <= ratio["standard"]:
                instance_type = "standard"
            else:
                instance_type = "highmem"

        # Initializing predefined instance data
        predef_inst = {}

        # Converting the number of cpus to the closest upper power of 2
        predef_inst["nr_cpus"] = 2 ** int(math.ceil(math.log(self.nr_cpus, 2)))

        # Computing the memory obtain on the instance
        predef_inst["mem"] = predef_inst["nr_cpus"] * ratio[instance_type]

        # Setting the instance type name
        predef_inst["type_name"] = "n1-%s-%d" % (instance_type, predef_inst["nr_cpus"])

        # Obtaining the price of the predefined instance
        if self.is_preemptible:
            predef_inst["price"] = prices["CP-COMPUTEENGINE-VMIMAGE-%s-PREEMPTIBLE" % predef_inst["type_name"].upper()]["us"]
        else:
            predef_inst["price"] = prices["CP-COMPUTEENGINE-VMIMAGE-%s" % predef_inst["type_name"].upper()]["us"]

        # Initializing custom instance data
        custom_inst = {}

        # Computing the number of cpus for a possible custom machine and making sure it's an even number or 1.
        if self.nr_cpus != 1:
            custom_inst["nr_cpus"] = self.nr_cpus + self.nr_cpus % 2
        else:
            custom_inst["nr_cpus"] = 1

        # Computing the memory as integer value in GB
        custom_inst["mem"] = int(math.ceil(self.mem))

        # Making sure the memory value is not under HIGHCPU and not over HIGHMEM
        custom_inst["mem"] = max(ratio["highcpu"] * custom_inst["nr_cpus"], custom_inst["mem"])
        if self.nr_cpus != 1:
            custom_inst["mem"] = min(ratio["highmem"] * custom_inst["nr_cpus"], custom_inst["mem"])
        else:
            custom_inst["mem"] = max(1, custom_inst["mem"])
            custom_inst["mem"] = min(6.5, custom_inst["mem"])

        # Generating custom instance name
        custom_inst["type_name"] = "custom-%d-%d" % (custom_inst["nr_cpus"], custom_inst["mem"])

        # Computing the price of a custom instance
        if self.is_preemptible:
            custom_price_cpu = prices["CP-COMPUTEENGINE-CUSTOM-VM-CORE-PREEMPTIBLE"]["us"]
            custom_price_mem = prices["CP-COMPUTEENGINE-CUSTOM-VM-RAM-PREEMPTIBLE"]["us"]
        else:
            custom_price_cpu = prices["CP-COMPUTEENGINE-CUSTOM-VM-CORE"]["us"]
            custom_price_mem = prices["CP-COMPUTEENGINE-CUSTOM-VM-RAM"]["us"]
        custom_inst["price"] = custom_price_cpu * custom_inst["nr_cpus"] + custom_price_mem * custom_inst["mem"]

        if predef_inst["price"] <= custom_inst["price"]:
            self.nr_cpus = predef_inst["nr_cpus"]
            self.mem = predef_inst["mem"]
            return predef_inst["type_name"]
        else:
            self.nr_cpus = custom_inst["nr_cpus"]
            self.mem = custom_inst["mem"]
            return custom_inst["type_name"]



