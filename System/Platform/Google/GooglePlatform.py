import os
import logging
import json
import subprocess as sp
import tempfile
import math

from System.Platform import Platform, Processor
from GoogleStandardProcessor import GoogleStandardProcessor
from GooglePreemptibleProcessor import GooglePreemptibleProcessor
from GoogleCloudHelper import GoogleCloudHelper

class GooglePlatform(Platform):

    CONFIG_SPEC = "System/Platform/GooglePlatform.validate"

    def __init__(self, name, platform_config_file, final_output_dir):
        # Call super constructor from Platform
        super(GooglePlatform, self).__init__(name, platform_config_file, final_output_dir)

        # Get google access fields from JSON file
        self.key_file       = self.config["service_account_key_file"]
        self.service_acct   = GoogleCloudHelper.get_field_from_key_file(self.key_file, field_name="client_email")
        self.google_project = GoogleCloudHelper.get_field_from_key_file(self.key_file, field_name="project_id")

        # Get Google compute zone from config
        self.zone = self.config["zone"]

        # Determine whether to distribute processors across zones randomly
        self.randomize_zone = self.config["randomize_zone"]

        # Obtain the reporting topic
        self.report_topic   = self.config["report_topic"]

        # Boolean for whether worker instance create by platform will be preemptible
        self.is_preemptible = self.config["is_preemptible"]

        # Use authentication key file to gain access to google cloud project using Oauth2 authentication
        GoogleCloudHelper.authenticate(self.key_file)

    def validate(self):
        # Check that final output dir begins with gs://
        if not self.final_output_dir.startswith("gs://"):
            logging.error("Invalid final output directory: %s. Google bucket paths must begin with 'gs://'"
                          % self.final_output_dir)
            raise IOError("Invalid final output directory!")

        # Make gs bucket if it doesn't exists already
        gs_bucket = GoogleCloudHelper.get_bucket_from_path(self.final_output_dir)
        if not GoogleCloudHelper.gs_path_exists(gs_bucket):
            region = GoogleCloudHelper.get_region(self.zone)
            GoogleCloudHelper.mb(gs_bucket, project=self.google_project, region=region)

        # Set the minimum disk size based on size of disk image
        disk_image = self.config["task_processor"]["disk_image"]
        disk_image_info = GoogleCloudHelper.get_disk_image_info(disk_image)
        self.MIN_DISK_SPACE = int(disk_image_info["diskSizeGb"])

        # Check to see if the reporting Pub/Sub topic exists
        if not GoogleCloudHelper.pubsub_topic_exists(self.report_topic):
            logging.error("Reporting topic '%s' was not found!")
            raise IOError("Reporting topic '%s' not found!")

    def init_helper_processor(self, name, nr_cpus, mem, disk_space):
        # Return processor object that will be used
        instance_config = self.__get_instance_config()
        name = self.__format_instance_name(name)
        return GoogleStandardProcessor(name, nr_cpus, mem, disk_space, **instance_config)

    def init_task_processor(self, name, nr_cpus, mem, disk_space):
        # Return a processor object with given resource requirements
        instance_config = self.__get_instance_config()
        name = self.__format_instance_name(name)

        # Create and return processor
        if self.is_preemptible:
            return GooglePreemptibleProcessor(name, nr_cpus, mem, disk_space, **instance_config)
        else:
            return GoogleStandardProcessor(name, nr_cpus, mem, disk_space, **instance_config)

    def publish_report(self, report=None):

        # Exit as nothing to output
        if report is None:
            return

        # Send report to the Pub/Sub report topic
        GoogleCloudHelper.send_pubsub_message(self.report_topic, message=report)

        # Generate report file for transfer
        with tempfile.NamedTemporaryFile(delete=False) as report_file:
            report_file.write(report)
            report_filepath = report_file.name

        # Transfer report file to bucket
        if report_filepath is not None:
            options_fast = '-m -o "GSUtil:sliced_object_download_max_components=200"'
            dest_path = os.path.join(self.final_output_dir, "%s_final_report.json" % self.name)
            cmd = "gsutil %s cp -r %s %s 1>/dev/null 2>&1 " % (options_fast, report_filepath, dest_path)
            GoogleCloudHelper.run_cmd(cmd, err_msg="Could not transfer final report to the final output directory!")

    def clean_up(self):

        logging.info("Cleaning up Google Cloud Platform.")
        # Remove dummy.txt from final output bucket
        try:
            cmd         = "gsutil rm %s" % os.path.join(self.final_output_dir,"dummy.txt")
            proc        = sp.Popen(cmd, stderr=sp.PIPE, stdout=sp.PIPE, shell=True)
            proc.communicate()
        except:
            logging.warning("(%s) Could not remove dummy input file on google cloud!")

        # Initiate destroy process on all the instances that haven't been destroyed
        for instance_name, instance_obj in self.processors.iteritems():
            try:
                if instance_obj.get_status() not in [Processor.DEAD, Processor.OFF]:
                    instance_obj.destroy(wait=False)
            except RuntimeError:
                logging.warning("(%s) Could not destroy instance!" % instance_name)

        # Now wait for all destroy processes to finish
        for instance_name, instance_obj in self.processors.iteritems():
            try:
                instance_obj.wait_process("destroy")
            except RuntimeError:
                logging.warning("(%s) Could not destroy instance!" % instance_name)

        logging.info("Clean up complete!")

    ####### PRIVATE UTILITY METHODS

    def __get_instance_config(self):
        # Returns complete config for a task processor
        params = {}
        inst_params = self.config["task_processor"]
        for param, value in inst_params.iteritems():
            params[param] = value

        # Add platform-specific options
        params["zone"]                  = self.zone
        params["PROC_MAX_NR_CPUS"]      = self.get_max_nr_cpus()
        params["PROC_MAX_MEM"]          = self.get_max_mem()
        params["PROC_MAX_DISK_SPACE"]   = self.get_max_disk_space()

        # Add name of service account
        params["service_acct"]          = self.service_acct

        # Randomize the zone within the region if specified
        if self.randomize_zone:
            region          = GoogleCloudHelper.get_region(self.zone)
            params["zone"]  = GoogleCloudHelper.select_random_zone(region)

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
