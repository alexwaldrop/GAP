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

    def __init__(self, name, nr_cpus, mem, disk_space, **kwargs):
        # Call super constructor
        super(GoogleStandardProcessor,self).__init__(name, nr_cpus, mem, disk_space, **kwargs)

        # Get required arguments
        self.zone               = kwargs.pop("zone")
        self.service_acct       = kwargs.pop("service_acct")
        self.disk_image         = kwargs.pop("disk_image")

        # Get optional arguments
        self.is_boot_disk_ssd   = kwargs.pop("is_boot_disk_ssd",    False)
        self.nr_local_ssd       = kwargs.pop("nr_local_ssd",        0)

        # Initialize the region of the instance
        self.region             = GoogleCloudHelper.get_region(self.zone)

        # Initialize instance random id
        self.rand_instance_id   = self.name.rsplit("-",1)[-1]

        # Indicates that instance is not resettable
        self.is_preemptible = False

        # Google instance type. Will be set at creation time based on google price scheme
        self.instance_type = None

        # Initialize the price of the run and the total cost of the run
        self.price = 0
        self.cost = 0

    def create(self):
        # Begin running command to create the instance on Google Cloud

        # Set status to indicate that commands can't be run on processor because it's busy
        self.set_status(GoogleStandardProcessor.BUSY)

        logging.info("(%s) Process 'create' started!" % self.name)
        # Determine instance type and actual resource usage based on current Google prices in instance zone
        self.nr_cpus, self.mem, self.instance_type = GoogleCloudHelper.get_optimal_instance_type(self.nr_cpus,
                                                                                                 self.mem,
                                                                                                 self.zone,
                                                                                                 self.is_preemptible)

        # Determine instance price at time of creation
        self.price = GoogleCloudHelper.get_instance_price(self.nr_cpus,
                                                          self.mem,
                                                          self.disk_space,
                                                          self.instance_type,
                                                          self.zone,
                                                          self.is_preemptible,
                                                          self.is_boot_disk_ssd,
                                                          self.nr_local_ssd)

        logging.debug("(%s) Instance type is %s. Price per hour: %s cents" % (self.name, self.instance_type, self.price))

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
        if self.disk_space >= 10240:
            args.append("%dTB" % int(math.ceil(self.disk_space/1024.0)))
        else:
            args.append("%dGB" % int(self.disk_space))

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
        if "custom" in self.instance_type:
            args.append("--custom-cpu")
            args.append(str(self.nr_cpus))

            args.append("--custom-memory")
            args.append("%sGB" % str(int(self.mem)))
        else:
            args.append("--machine-type")
            args.append(self.instance_type)

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

        # Increase number of ssh connections
        self.configure_SSH()

        # Configure CRCMOD for fast file transfer
        self.configure_CRCMOD()

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
                             num_retries=proc_obj.get_num_retries()-1,
                             docker_image=proc_obj.get_docker_image(),
                             quiet_failure=proc_obj.is_quiet())
                    return self.wait_process(proc_name)

                # Throw error if no retries left
                else:
                    # Log failure to debug logger if quiet failure
                    if proc_obj.is_quiet():
                        logging.debug("(%s) Process '%s' failed!" % (self.name, proc_name))
                        logging.debug("(%s) The following error was received: \n  %s\n%s" % (self.name, out, err))
                    # Otherwise log failure to error logger
                    else:
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

    def configure_CRCMOD(self, log=False):
        # Install necessary packages
        self.install_packages(["gcc", "python-dev", "python-setuptools"], log=log)

        self.set_status(GoogleStandardProcessor.BUSY)

        # Install CRCMOD python package
        logging.info("(%s) Configuring CRCMOD for fast data tranfer using gsutil." % self.name)
        if log:
            cmd = "python -c 'import crcmod' || (sudo easy_install -U pip !LOG3! && sudo pip uninstall -y crcmod !LOG3! && sudo pip install -U crcmod !LOG3!)"
        else:
            cmd = "python -c 'import crcmod' || (sudo easy_install -U pip && sudo pip uninstall -y crcmod && sudo pip install -U crcmod)"
        self.run("configCRCMOD_%s" % self.rand_instance_id, cmd)
        self.wait_process("configCRCMOD_%s" % self.rand_instance_id)

        self.set_status(GoogleStandardProcessor.AVAILABLE)

    def install_packages(self, packages, log=False):
        # If no packages are provided to install
        if not packages:
            return

        if not isinstance(packages, list):
            packages = [packages]

        # Log installation
        logging.info("(%s) Installing the following packages: %s" % (self.name, " ".join(packages)))

        self.set_status(GoogleStandardProcessor.BUSY)

        # Get command to install packages
        if log:
            cmd         = "yes | sudo aptdcon --hide-terminal -i \"%s\" !LOG3! " % " ".join(packages)
        else:
            cmd         = "yes | sudo aptdcon --hide-terminal -i \"%s\" " % " ".join(packages)
        # Create random id for job
        job_name    = "install_packages_%d_%s" % (random.randint(1,100000), self.rand_instance_id)
        self.run(job_name, cmd)
        self.wait_process(job_name)

        self.set_status(GoogleStandardProcessor.AVAILABLE)

    def configure_SSH(self, max_connections=500, log=False):

        self.set_status(GoogleStandardProcessor.BUSY)

        # Increase the number of concurrent SSH connections
        logging.info("(%s) Increasing the number of maximum concurrent SSH connections to %s." % (self.name, max_connections))
        if log:
            cmd = "sudo bash -c 'echo \"MaxStartups %s\" >> /etc/ssh/sshd_config' !LOG2! " % max_connections
        else:
            cmd = "sudo bash -c 'echo \"MaxStartups %s\" >> /etc/ssh/sshd_config' " % max_connections
        self.run("configureSSH_%s" % self.rand_instance_id, cmd)
        self.wait_process("configureSSH_%s" % self.rand_instance_id)

        # Restart SSH daemon to load the settings
        logging.info("(%s) Restarting SSH daemon to load the new settings." % self.name)
        if log:
            cmd = "sudo service sshd restart !LOG3!"
        else:
            cmd = "sudo service sshd restart"
        self.run("restartSSH_%s" % self.rand_instance_id, cmd)
        self.wait_process("restartSSH_%s" % self.rand_instance_id)

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

    def exists(self):

        # Check if the current instance still exists on the platform
        cmd = 'gcloud compute instances list | grep "%s"' % self.name
        out, _ = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True).communicate()
        return len(out) != 0
