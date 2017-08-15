import os
import logging
import json
import subprocess as sp

from GoogleStandardProcessor import GoogleStandardProcessor
from GooglePreemptibleProcessor import GooglePreemptibleProcessor
from PreemptionNotifier import PreemptionNotifier
from System.Platform import Platform

class GooglePlatform(Platform):
    def __init__(self, name, platform_config_file, final_output_dir):
        # Call super constructor from Platform
        super(GooglePlatform, self).__init__(name, platform_config_file, final_output_dir)

        # Get google access fields from JSON file
        self.key_file       = self.config["global"]["service_account_key_file"]
        self.service_acct   = self.__get_key_field("client_email")
        self.google_project = self.__get_key_field("project_id")

        # Get Google compute zone from config
        self.zone = self.config["global"]["zone"]

        # Use authentication key file to gain access to google cloud project using Oauth2 authentication
        self.authenticate()

        # Do pre-launch validation
        self.__do_prelaunch_check()

        # Boolean for whether worker instance create by platform will be preemptible
        self.is_preemptible = self.config["worker_instance"]["is_preemptible"]

        # Start preemption notifier daemon if child instances are preemptible
        self.preemption_notifier = None
        if self.is_preemptible:
            # Create preemption notifier
            self.preemption_notifier = PreemptionNotifier(name, self.processors, self.key_file)
            # Attempt to start the preemption notifier
            try:
                self.preemption_notifier.start()
            except:
                logging.error("Unable to start preemption notifier!")
                self.preemption_notifier.clean_up()
                raise

    def define_config_spec_file(self):
        # Return path to config spec file used to validate platform config
        return "Config/Schema/Platform/GooglePlatform.validate"

    def authenticate(self):

        logging.info("Authenticating to the Google Cloud.")

        if not os.path.exists(self.key_file):
            logging.error("Authentication key was not found!")
            exit(1)

        cmd = "gcloud auth activate-service-account --key-file %s" % self.key_file
        with open(os.devnull, "w") as devnull:
            proc = sp.Popen(cmd, stdout=devnull, stderr=devnull, shell=True)

        if proc.wait() != 0:
            logging.error("Authentication to Google Cloud failed!")
            exit(1)

        logging.info("Authentication to Google Cloud was successful.")

    def create_main_processor(self):
        # Initialize and return the main processor needed to load/manage the platform
        logging.info("Creating main platform processing instance...")

        # Get parameters for creating main instance
        main_instance_config = self.__get_instance_config(is_main_instance=True)

        # Add parameter for log directory
        main_instance_config["log_dir"] = self.get_workspace_dir("log")

        # Get name, nr_cpus, mem and instantiate main instance object
        name            = self.__format_instance_name("%s-main" % self.name)
        nr_cpus         = main_instance_config.pop("nr_cpus")
        mem             = main_instance_config.pop("mem")
        main_instance   = GoogleStandardProcessor(name, nr_cpus, mem, **main_instance_config)

        # Add main instance to list of processor
        self.processors[main_instance.get_name()]   = main_instance
        self.main_processor                         = main_instance

        # Create main instance and wait for creation to complete
        main_instance.create()

        # Configure SSH on main instance
        main_instance.configure_SSH()

        # Configure crcmod for fast transfer
        main_instance.configure_CRCMOD()

        # Install packages to main instance
        packages_to_install = main_instance_config.get("apt_packages")
        main_instance.install_packages(packages_to_install)

        # Return main instance
        return main_instance

    def create_processor(self, name, nr_cpus, mem):

        # Return a processor ready to run a process requiring the given amount of CPUs and Memory
        # Get parameters for creating worker instance
        instance_config = self.__get_instance_config(is_main_instance=False)

        # Correct name formatting
        name = self.__format_instance_name(name)

        # Add parameter for log directory
        instance_config["log_dir"] = self.get_workspace_dir("log")

        # Create preemptible or standard instance from value in config
        if self.is_preemptible:
            instance   = GooglePreemptibleProcessor(name, nr_cpus, mem, **instance_config)
        else:
            instance   = GoogleStandardProcessor(name, nr_cpus, mem, **instance_config)

        # Add to list of processors
        self.processors[instance.get_name()] = instance

        # Create main instance and wait for creation to complete
        instance.create()

        # Configure SSH on main instance
        instance.configure_SSH()

        # Configure crcmod for fast transfer
        instance.configure_CRCMOD()

        # Make workspace directory on new instance
        cmd = "sudo mkdir -p %s" % self.get_workspace_dir()
        instance.run("mk_wrk_dir", cmd)
        instance.wait_process("mk_wrk_dir")

        # Mount workspace dir to main instance workspace dir using NFS
        instance.mount(parent_instance_name=self.main_processor.get_name(),
                       parent_mount_point=self.get_workspace_dir(),
                       child_mount_point=self.get_workspace_dir())

        # Install packages to instance
        packages_to_install = instance_config.get("apt_packages")
        instance.install_packages(packages_to_install)

        # Return main instance
        return instance

    def init_workspace(self):
        # Create the workspace directories with the main processor
        # Create main workspace directory
        self.mkdir(self.get_workspace_dir(), job_name="mk_wrkspace_wrk")

        # Configure RAID storage system system
        logging.info("Configuring RAID on main server...")
        self.main_processor.configure_RAID(raid_dir=self.get_workspace_dir())

        # Create workspace subdirectories
        for dir_type in self.workspace:
            if dir_type != "wrk":
                logging.info("Creating %s directory..." % dir_type)
                self.mkdir(self.get_workspace_dir(dir_type), job_name="mk_wrkspace_%s" % dir_type)

        logging.info("Configuring NFS on main server...")
        # Configure NFS to allow child instances to mount main instance
        self.main_processor.configure_NFS(exported_dir=self.get_workspace_dir())

        # Make the entire workspace directory accessible to everyone
        logging.info("Updating workspace permissions...")
        cmd = "sudo chmod -R 777 %s" % self.get_workspace_dir()
        self.main_processor.run("update_wrkspace_perms", cmd)

        # Wait for all commands to complete
        self.main_processor.wait()
        logging.info("Workspace initialized successfully!")

    def path_exists(self, path, job_name=None):
        # Determine if a path exists either locally on platform or remotely
        job_name = "check_exists_%s" % self.generate_unique_id() if job_name is None else job_name
        if ":" in path:
            # Check if path exists on google bucket storage
            cmd         = "gsutil ls %s" % path
            proc        = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
        else:
            # Check if file exists locally on main instance
            cmd     = "ls %s" % path
            proc    = self.main_processor.run(job_name, cmd)
        out, err = proc.communicate()
        return len(err) == 0

    def transfer(self, src_path, dest_dir, dest_file=None, log_transfer=True, job_name=None, wait=False):
        # Transfer a remote file from src_path to a local directory dest_dir
        # Log the transfer unless otherwise specified

        # Create job name
        job_name        = "transfer_%s" % self.generate_unique_id() if job_name is None else job_name

        # Google cloud options for fast transfer
        options_fast    = '-m -o "GSUtil:sliced_object_download_max_components=200"'

        # Specify whether to log transfer
        log_flag        = "!LOG3!" if log_transfer else ""

        # Specify destination of transfer
        dest_dir        = self.standardize_dir(dest_dir)
        dest_path       = dest_dir if dest_file is None else os.path.join(dest_dir, dest_file)

        # Run command to copy file
        cmd             = "gsutil %s cp -r %s %s %s" % (options_fast, src_path, dest_path, log_flag)
        self.main_processor.run(job_name, cmd)
        if wait:
            self.main_processor.wait_process(job_name)

    def mkdir(self, dir_path, job_name=None, wait=False):
        # Makes a directory if it doesn't already exists
        # Standardize dir_path
        dir_path = self.standardize_dir(dir_path)
        job_name = "mkdir_%s" % self.generate_unique_id() if job_name is None else job_name

        if ":" in dir_path:
            # Make bucket if it doesn't already exist on google cloud
            bucket = "/".join(dir_path.split("/")[0:3]) + "/"
            if not self.path_exists(bucket):
                logging.debug("Creating final output bucket: %s" % bucket)
                bucket_job_name = "mk_bucket_%s" % bucket
                region      = "-".join(self.zone.split("-")[:-1])
                cmd         = "gsutil mb -p %s -c regional -l %s %s" % (self.google_project, region, bucket)
                self.run_quick_command(bucket_job_name, cmd)
            # Generate command to add dummy file to bucket directory and delete local copy
            logging.debug("Creating dummy output file in final output dir on google storage...")
            cmd = "touch dummy.txt ; gsutil cp dummy.txt %s" % dir_path
            self.main_processor.run(job_name, cmd)
        else:
            # Generate command to make directory locally on the main instance
            cmd = "sudo mkdir -p %s" % dir_path

        # Run command
        self.main_processor.run(job_name, cmd)

        # Optionally wait for command to finish
        if wait:
            self.main_processor.wait_process(job_name)

    def clean_up(self):
        logging.info("Cleaning up Google Cloud Platform.")

        # Remove dummy.txt from final output bucket
        try:
            cmd         = "gsutil rm %s" % os.path.join(self.final_output_dir,"dummy.txt")
            proc        = sp.Popen(cmd, stderr=sp.PIPE, stdout=sp.PIPE, shell=True)
            out, err    = proc.communicate()
        except:
            logging.info("(%s) Could not remove dummy input file on google cloud!")

        # Initiate destroy process on all the instances except the main processor
        for instance_name, instance_obj in self.processors.iteritems():
            if instance_name != self.main_processor.get_name():
                try:
                    instance_obj.destroy(wait=False)
                except RuntimeError:
                    logging.info("(%s) Could not destroy instance!" % instance_name)

        # Wait for all instances to be destroyed
        for instance_name, instance_obj in self.processors.iteritems():
            if instance_name != self.main_processor.get_name():
                try:
                    instance_obj.wait_process("destroy")
                except RuntimeError:
                    logging.info("(%s) Could not destroy instance!" % instance_name)


        # Destroy main instance last
        if self.main_processor is not None:
            self.main_processor.destroy()

        # Destroy preemption notifier if necessary
        if self.preemption_notifier is not None:
            self.preemption_notifier.clean_up()

        logging.info("Clean up complete!")

    ####### PRIVATE UTILITY METHODS
    def __get_key_field(self, field_name):
        # Parse JSON service account key file and return email address associated with account
        logging.info("Extracting %s from JSON key file." % field_name)

        if not os.path.exists(self.key_file):
            logging.error("Google authentication key file not found: %s!" % self.key_file)
            raise IOError("Google authentication key file not found!")

        # Parse json into dictionary
        with open(self.key_file) as kf:
            key_data = json.load(kf)

        # Check to make sure correct key is present in dictionary
        if field_name not in key_data:
            logging.error(
                "'%s' field missing from authentication key file: %s. Check to make sure key exists in file or that file is valid google key file!"
                % (field_name, self.key_file))
            raise IOError("Info field not found in Google key file!")
        return key_data[field_name]

    def __get_instance_config(self, is_main_instance=True):
        # Returns complete config for main-instance processor
        params = {}

        # Add global parameters
        for param, value in self.config["global"].iteritems():
            params[param] = value

        # Add parameters specific to the instance type
        inst_params = self.config["main_instance"] if is_main_instance else self.config["worker_instance"]
        for param, value in inst_params.iteritems():
            params[param] = value

        # Add Max CPU/MEM platform values
        params["PROC_MAX_NR_CPUS"]  = self.get_max_nr_cpus()
        params["PROC_MAX_MEM"]      = self.get_max_mem()

        # Add name of service account
        params["service_acct"]      = self.service_acct

        return params

    @staticmethod
    def __format_instance_name(instance_name):
        # Ensures that instance name conforms to google cloud formatting specs
        old_instance_name = instance_name
        instance_name = instance_name.replace("_", "-")
        instance_name = instance_name.replace(".", "-")
        instance_name = instance_name.lower()

        # Declare if name of instance has changed
        if old_instance_name != instance_name:
            logging.warn("Modified instance name from %s to %s for compatibility!" % (old_instance_name, instance_name))

        return instance_name

    ####### GOOGLE SPECIFIC FUNCTIONS

    def __do_prelaunch_check(self):
        # Check that final output dir begins with gs://
        if not self.final_output_dir.startswith("gs://"):
            logging.error("Invalid final output directory: %s. Google bucket paths must begin with 'gs://'"
                          % self.final_output_dir)
            raise IOError("Invalid final output directory!")

        # Check to see if disk image exists and is larger than requested boot disk size
        self.__validate_disk_image("Main Instance",
                                 disk_image_name=self.config["main_instance"]["disk_image"],
                                 boot_disk_size=self.config["main_instance"]["boot_disk_size"])

        # Check to see if split disk image exists and is larger than requested split boot disk
        self.__validate_disk_image("Worker Instance",
                                 disk_image_name=self.config["worker_instance"]["disk_image"],
                                 boot_disk_size=self.config["worker_instance"]["boot_disk_size"])

    def __validate_disk_image(self, disk_image_type, disk_image_name, boot_disk_size):
        # Checks to make sure that disk image exists and is smaller than a given boot disk size
        logging.info("Checking existence of %s disk image: %s." % (disk_image_type, disk_image_name))

        # Get disk image info
        disk_image_info = self.__get_disk_image_info(disk_image_name)

        # Throw error if not found
        if disk_image_info is None:
            logging.error("Unable to find %s disk image: %s!" % (disk_image_type, disk_image_name))
            raise IOError("Invalid disk image provided in GooglePlatform config!")

        # Get size of disk image
        disk_image_size = int(disk_image_info["diskSizeGb"])

        # Throw error if size of boot disk is smaller than the size of the instance
        if boot_disk_size < disk_image_size:
            logging.error("%s boot disk size is smaller (%dGB) than the disk image requested (%s: %dGB). "
                          "Increase size of %s boot disk in config!"
                          %(disk_image_type, boot_disk_size, disk_image_name, disk_image_size, disk_image_type))
            raise IOError("Invalid boot disk size provided in GooglePlatform config!")

    @staticmethod
    def __get_disk_image_info(disk_image_name):
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




