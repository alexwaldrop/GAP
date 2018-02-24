import logging
import subprocess as sp
import threading
import random
import time
import sys
import math
import json

from System.Platform import Process
from System.Platform import Processor
from System.Platform.Google import GoogleCloudHelper

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

        # Initialize the region of the instance
        self.region             = GoogleCloudHelper.get_region(self.zone)

        # Initialize the list of attached disks
        self.disks              = []

        # Indicates that instance is not resettable
        self.is_preemptible = False

        # Initialize the price of the run and the total cost of the run
        self.price = 0
        self.cost = 0

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

    def attach_disk(self, name):

        logging.info("(%s) Attaching disk '%s'." % (self.name, name))

        # Generate attaching command
        cmd = "gcloud compute instances attach-disk %s --disk=%s --zone %s --device-name=gap_disk_%s" \
              % (self.name, name, self.zone, name)

        # Run command
        self.processes["attach_disk"] = Process(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
        self.wait_process("attach_disk")

        # Register the attached disk
        self.disks.append(name)

    def detach_disk(self, name):

        logging.info("(%s) Detaching disk '%s'." % (self.name, name))

        # Generate detaching command
        cmd = "gcloud compute instances detach-disk %s --disk %s --zone %s" % (self.name, name, self.zone)

        # Run the command
        self.processes["detach_disk"] = Process(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
        self.wait_process("detach_disk")

        # Remove disk from the list
        self.disks.remove(name)

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
        if self.boot_disk_size >= 10240:
            args.append("%dTB" % int(math.ceil(self.boot_disk_size/1024.0)))
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
            args.append("%sGB" % str(int(self.mem)))
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
        if self.get_status() == GoogleStandardProcessor.OFF and not self.exists():
            return

        # Set status to indicate that instance cannot run commands and is destroying
        self.set_status(GoogleStandardProcessor.BUSY)

        logging.info("(%s) Process 'destroy' started!" % self.name)

        # Detaching disks one-by-one
        while self.disks:
            self.detach_disk(self.disks[0])

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
                # Check to see if command can be re-tried
                if proc_obj.get_num_retries() > 0 and self.get_status() != GoogleStandardProcessor.OFF:
                    # Retry same command and decriment num_retries
                    logging.warning("(%s) Process '%s' failed but we still got %s retries left. Re-running command!" % (self.name, proc_name, proc_obj.get_num_retries()))
                    self.run(job_name=proc_name,
                             cmd=proc_obj.get_command(),
                             num_retries=proc_obj.get_num_retries()-1)
                    return self.wait_process(proc_name)
                else:
                    # Throw error if no retries left
                    logging.error("(%s) Process '%s' failed!" % (self.name, proc_name))
                    logging.error("(%s) The following error was received: \n  %s\n%s" % (self.name, out, err))
                    raise RuntimeError("Instance %s has failed!" % self.name)

        # Set the start time
        if proc_name == "create":
            self.set_start_time()

        # Set status to 'OFF' if destroy is True
        if proc_name == "destroy":
            self.set_status(GoogleStandardProcessor.OFF)

            # Set the stop time
            self.set_stop_time()

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

        self.set_status(GoogleStandardProcessor.BUSY)

        # Generate command for mounting main instance
        logging.info("(%s) Mounting to %s." % (self.name, parent_instance_name))
        cmd = "sudo mkdir -p %s && sudo mount -t nfs %s:%s %s !LOG0!" % (child_mount_point,
                                                                         parent_instance_name,
                                                                         parent_mount_point,
                                                                         child_mount_point)
        # Run command and return when complete
        self.run("mountNFS", cmd)
        self.wait_process("mountNFS")

        self.set_status(GoogleStandardProcessor.AVAILABLE)

    def configure_CRCMOD(self):
        # Install necessary packages
        self.install_packages(["gcc", "python-dev", "python-setuptools"])

        self.set_status(GoogleStandardProcessor.BUSY)

        # Install CRCMOD python package
        logging.info("(%s) Configuring CRCMOD for fast data tranfer using gsutil." % self.name)
        cmd = "python -c 'import crcmod' 2>/dev/null || (sudo easy_install -U pip && sudo pip uninstall -y crcmod && sudo pip install -U crcmod)"
        self.run("configCRCMOD", cmd)
        self.wait_process("configCRCMOD")

        self.set_status(GoogleStandardProcessor.AVAILABLE)

    def install_packages(self, packages):
        # If no packages are provided to install
        if not packages:
            return

        if not isinstance(packages, list):
            packages = [packages]

        # Log installation
        logging.info("(%s) Installing the following packages: %s" % (self.name, " ".join(packages)))

        self.set_status(GoogleStandardProcessor.BUSY)

        # Get command to install packages
        cmd         = "yes | sudo aptdcon --hide-terminal -i \"%s\" !LOG0! " % " ".join(packages)
        # Create random id for job
        job_name    = "install_packages_%d" % random.randint(1,100000)
        self.run(job_name, cmd)
        self.wait_process(job_name)

        self.set_status(GoogleStandardProcessor.AVAILABLE)

    def configure_SSH(self, max_connections=500):

        self.set_status(GoogleStandardProcessor.BUSY)

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

        self.set_status(GoogleStandardProcessor.AVAILABLE)

    def adapt_cmd(self, cmd):
        # Adapt command for running on instance through gcloud ssh
        cmd = cmd.replace("'", "'\"'\"'")
        cmd = "gcloud compute ssh gap@%s --command '%s' --zone %s" % (self.name, cmd, self.zone)
        return cmd

    def is_fatal_error(self, proc_name, err_msg):
        # Check to see if program should exit due to error received

        # Check if 'destroy' process actually deleted the instance, in which case program can continue running
        if proc_name == "destroy" and not self.exists():
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

        # Reset if instance not initialized within the alloted timeframe
        if not ready:
            logging.info("(%s) Instance failed! 'Create' Process took more than 20 minutes! "
                         "The instance will be reset!" % self.name)
            self.destroy()
            self.create()

    def configure_NFS(self, exported_dir):

        # Install required packages
        self.install_packages(["sysv-rc-conf", "nfs-kernel-server"])

        self.set_status(GoogleStandardProcessor.BUSY)

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

        self.set_status(GoogleStandardProcessor.AVAILABLE)

    def configure_RAID(self, raid_dir):

        # Check if there are any localSSDs
        if self.nr_local_ssd == 0:
            logging.info("(%s) RAID-0 will not be configured as there are no LocalSSDs.")
            return

        # Install the required packages
        self.install_packages("mdadm")

        self.set_status(GoogleStandardProcessor.BUSY)

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

        self.set_status(GoogleStandardProcessor.AVAILABLE)

    def configure_DISK(self, work_dir):

        self.set_status(GoogleStandardProcessor.BUSY)

        # Format the workspace disk
        logging.info("(%s) Formating workspace disk." % self.name)
        cmd = "sudo mkfs -t ext4 $(ls /dev/disk/by-id/* | grep google-gap_disk)"
        self.run("formatDISK", cmd)
        self.wait_process("formatDISK")

        # Mount the RAID partition
        logging.info("(%s) Mounting workspace disk." % self.name)
        cmd = "sudo mkdir -p %s && sudo mount -t ext4 $(ls /dev/disk/by-id/* | grep google-gap_disk) %s" \
              % (work_dir, work_dir)
        self.run("mountDISK", cmd)
        self.wait_process("mountDISK")

        # Change permission on the the RAID partition
        logging.info("(%s) Changing permissions for the workspace disk." % self.name)
        cmd = "sudo chmod -R 777 %s" % work_dir
        self.run("chmodDISK", cmd)
        self.wait_process("chmodDISK")

        self.set_status(GoogleStandardProcessor.AVAILABLE)

    def get_instance_type(self):

        # Making sure the values are not higher than possibly available
        if self.nr_cpus > self.MAX_NR_CPUS:
            logging.error("(%s) Cannot provision an instance with %d vCPUs. Maximum is %d vCPUs." % (self.name, self.nr_cpus, self.MAX_NR_CPUS))
            raise RuntimeError("Instance %s has failed!" % self.name)
        if self.mem > self.MAX_MEM:
            logging.error("(%s) Cannot provision an instance with %d GB RAM. Maximum is %d GB RAM." % (self.name, self.mem, self.MAX_MEM))
            raise RuntimeError("Instance %s has failed!" % self.name)

        # Obtaining prices from Google Cloud Platform
        prices = GoogleCloudHelper.get_prices()

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

        # Obtain available machine type in the current zone
        machine_types = GoogleCloudHelper.get_machine_types(self.zone)

        # Initializing predefined instance data
        predef_inst = {}

        # Obtain the machine type that has the closes number of CPUs
        predef_inst["nr_cpus"] = sys.maxsize
        for machine_type in machine_types:

            # Skip instances that are not of the same type
            if instance_type not in machine_type["name"]:
                continue

            # Select the instance if its number of vCPUs is closer to the required nr_cpus
            if machine_type["guestCpus"] >= self.nr_cpus and machine_type["guestCpus"] < predef_inst["nr_cpus"]:
                predef_inst["nr_cpus"] = machine_type["guestCpus"]
                predef_inst["mem"] = machine_type["memoryMb"] / 1024
                predef_inst["type_name"] = machine_type["name"]

        # Obtaining the price of the predefined instance
        if self.is_preemptible:
            predef_inst["price"] = prices["CP-COMPUTEENGINE-VMIMAGE-%s-PREEMPTIBLE" % predef_inst["type_name"].upper()][self.region]
        else:
            predef_inst["price"] = prices["CP-COMPUTEENGINE-VMIMAGE-%s" % predef_inst["type_name"].upper()][self.region]

        # Initializing custom instance data
        custom_inst = {}

        # Computing the number of cpus for a possible custom machine and making sure it's an even number or 1.
        custom_inst["nr_cpus"] = 1 if self.nr_cpus == 1 else self.nr_cpus + self.nr_cpus%2

        # Making sure the memory value is not under HIGHCPU and not over HIGHMEM
        if self.nr_cpus != 1:
            mem = max(ratio["highcpu"] * custom_inst["nr_cpus"], self.mem)
            mem = min(ratio["highmem"] * custom_inst["nr_cpus"], mem)
        else:
            mem = max(1, self.mem)
            mem = min(6, mem)

        # Computing the ceil of the current memory
        custom_inst["mem"] = int(math.ceil(mem))

        # Generating custom instance name
        custom_inst["type_name"] = "custom-%d-%d" % (custom_inst["nr_cpus"], custom_inst["mem"])

        # Computing the price of a custom instance
        if self.is_preemptible:
            custom_price_cpu = prices["CP-COMPUTEENGINE-CUSTOM-VM-CORE-PREEMPTIBLE"][self.region]
            custom_price_mem = prices["CP-COMPUTEENGINE-CUSTOM-VM-RAM-PREEMPTIBLE"][self.region]
        else:
            custom_price_cpu = prices["CP-COMPUTEENGINE-CUSTOM-VM-CORE"][self.region]
            custom_price_mem = prices["CP-COMPUTEENGINE-CUSTOM-VM-RAM"][self.region]
        custom_inst["price"] = custom_price_cpu * custom_inst["nr_cpus"] + custom_price_mem * custom_inst["mem"]

        if predef_inst["price"] <= custom_inst["price"]:
            self.nr_cpus = predef_inst["nr_cpus"]
            self.mem = predef_inst["mem"]
            instance_type = predef_inst["type_name"]
            self.price += predef_inst["price"]

        else:
            self.nr_cpus = custom_inst["nr_cpus"]
            self.mem = custom_inst["mem"]
            instance_type = custom_inst["type_name"]
            self.price += custom_inst["price"]

        # Identify the price of the instance's disk
        if self.is_boot_disk_ssd:
            pd_price = prices["CP-COMPUTEENGINE-STORAGE-PD-SSD"][self.region] * self.boot_disk_size / 730.0
        else:
            pd_price = prices["CP-COMPUTEENGINE-STORAGE-PD-CAPACITY"][self.region] * self.boot_disk_size / 730.0
        self.price += pd_price

        # Identify the price of the local SSDs if present
        ssd_price = 0
        if self.nr_local_ssd:
            if self.is_preemptible:
                ssd_price = self.nr_local_ssd * prices["CP-COMPUTEENGINE-LOCAL-SSD-PREEMPTIBLE"][self.region] * 375
            else:
                ssd_price = self.nr_local_ssd * prices["CP-COMPUTEENGINE-LOCAL-SSD"][self.region] * 375
        self.price += ssd_price

        return instance_type

    def get_runtime_and_cost(self):

        runtime = self.get_runtime()

        self.cost = self.price * runtime / 3600

        return runtime, self.cost

    def exists(self):

        # Check if the current instance still exists on the platform
        cmd = 'gcloud compute instances list | grep "%s"' % self.name
        out, _ = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True).communicate()
        return len(out) != 0
