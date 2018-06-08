import logging
import subprocess as sp
import json
import random
import requests
import base64
import os
import sys
import math

class GoogleCloudHelperError(Exception):
    pass


class GoogleCloudHelper:

    prices = None
    machine_types = None

    @staticmethod
    def run_cmd(cmd, err_msg=None):

        # Running and waiting for the command
        proc = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
        out, err = proc.communicate()

        # Check if any error has appeared
        if len(err) != 0 and "error" in err.lower():
            logging.error("GoogleCloudHelper could not run the following command:\n%s" % cmd)
            if err_msg is not None:
                logging.error("%s. The following error appeared:\n    %s" % (err_msg, err))
            raise RuntimeError("GoogleCloudHelper command error!")

        return out

    @staticmethod
    def get_active_zones(region=None):
        # Return list of zones in a region

        # Run command to get list of zones
        cmd         = "gcloud compute zones list --format=json"
        err_msg     = "Unable to list zones within the current project!"
        out         = GoogleCloudHelper.run_cmd(cmd, err_msg)

        # Parse json
        zones       = []
        msg_json    = json.loads(out)
        for zone in msg_json:
            # Get region and zone name
            zone_name = zone["name"]
            region_name = GoogleCloudHelper.get_region(zone_name)
            status      = zone["status"]

            # Skip if zone isn't up
            if status != "UP":
                continue

            # Skip if the zone isn't within
            if region is not None and region_name != region:
                continue

            # Add zone to list of zones if its active and falls within filter region (or no filter region is provided)
            zones.append(zone["name"])
        return zones

    @staticmethod
    def get_region(zone):
        return "-".join(zone.split("-")[0:2])

    @staticmethod
    def select_random_zone(region):
        # Return a random active zone within a Compute region
        avail_zones = GoogleCloudHelper.get_active_zones(region)

        # Make sure there is at least one active zone
        if len(avail_zones) == 0:
            logging.error("Unable to find available zone within region: %s" % region)
            raise GoogleCloudHelperError("GoogleCloudHelper failed to select random zone!")

        # Return randomly selected zone from list of active zones within region
        new_zone = avail_zones[random.randint(0, len(avail_zones) - 1)]
        return new_zone

    @staticmethod
    def get_prices():

        if GoogleCloudHelper.prices:
            return GoogleCloudHelper.prices

        try:
            price_json_url = "https://cloudpricingcalculator.appspot.com/static/data/pricelist.json"

            # Disabling low levels of logging from module requests
            logging.getLogger("requests").setLevel(logging.WARNING)

            GoogleCloudHelper.prices = requests.get(price_json_url).json()["gcp_price_list"]

            return GoogleCloudHelper.prices
        except BaseException as e:
            if e.message != "":
                logging.error("Could not obtain instance prices. The following error appeared: %s." % e.message)
            raise

    @staticmethod
    def get_machine_types(zone):

        if GoogleCloudHelper.machine_types:
            return GoogleCloudHelper.machine_types

        cmd = "gcloud compute machine-types list --filter='zone:(%s)' --format=json" % zone
        machine_types = GoogleCloudHelper.run_cmd(cmd, err_msg="Cannot obtain machine types on GCP")

        GoogleCloudHelper.machine_types = json.loads(machine_types)

        return GoogleCloudHelper.machine_types

    @staticmethod
    def get_instance_status(name, zone):
        # Check status of instance
        cmd = 'gcloud compute instances describe %s --format json --zone %s' % (name, zone)
        out = GoogleCloudHelper.run_cmd(cmd, err_msg="Unable to get status for instance '%s'!" % name)

        # Read the status returned by Google
        msg_json = json.loads(out)
        if "status" not in msg_json:
            logging.error("Invalid description recieved for instance '%s'! 'status' not listed as a key!" % name)
            logging.error("Received following description:\n%s" % out)
            raise RuntimeError("Instance '%s' failed!" % name)

        # Return instance status
        return msg_json["status"]

    @staticmethod
    def send_pubsub_message(topic, message=None, attributes=None, encode=True):
        # Send a message to an existing Google cloud Pub/Sub topic

        # Return if message and attributes are both empty
        if message is None and attributes is None:
            return

        # Parse the input message and attributes
        message = "" if message is None else message
        attributes = {} if attributes is None else attributes

        # Encode the message if needed
        if encode:
            message = base64.b64encode(message)

        # Parse the attributes and pack into a single data structure message
        attrs = ",".join(["%s=%s" % (str(k), str(v)) for k, v in attributes.iteritems()])

        # Run command to publish message to the topic
        cmd = "gcloud --quiet --no-user-output-enabled beta pubsub topics publish %s \"%s\" --attribute=%s" \
              % (topic, message, attrs)

        err_msg = "Could not send a message to Google Pub/Sub"
        GoogleCloudHelper.run_cmd(cmd, err_msg=err_msg)

    @staticmethod
    def authenticate(key_file):
        # Attempt to authenticate GoogleCloud account from key_file
        logging.info("Authenticating to the Google Cloud.")
        if not os.path.exists(key_file):
            logging.error("Authentication key was not found: %s" % key_file)
            raise IOError("Authentication key file not found!")

        cmd = "gcloud auth activate-service-account --key-file %s" % key_file
        GoogleCloudHelper.run_cmd(cmd, "Authentication to Google Cloud failed!")

        logging.info("Authentication to Google Cloud was successful.")

    @staticmethod
    def get_disk_image_info(disk_image_name):
        # Returns information about a disk image for a project. Returns none if no image exists with the name.
        cmd = "gcloud compute images list --format=json"
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
        out, err = proc.communicate()

        # Load results into json
        disk_images = json.loads(out.rstrip())
        for disk_image in disk_images:
            if disk_image["name"] == disk_image_name:
                return disk_image

        # Throw error because disk image can't be found
        logging.error("Unable to find disk image '%s'" % disk_image_name)
        raise GoogleCloudHelperError("Invalid disk image provided in GooglePlatform config!")

    @staticmethod
    def get_field_from_key_file(key_file, field_name):
        # Parse JSON service account key file and return email address associated with account
        logging.info("Extracting %s from JSON key file." % field_name)

        if not os.path.exists(key_file):
            logging.error("Google authentication key file not found: %s!" % key_file)
            raise IOError("Google authentication key file not found!")

        # Parse json into dictionary
        with open(key_file) as kf:
            key_data = json.load(kf)

        # Check to make sure correct key is present in dictionary
        if field_name not in key_data:
            logging.error(
                "'%s' field missing from authentication key file: %s. Check to make sure key exists in file or that file is valid google key file!"
                % (field_name, key_file))
            raise IOError("Info field not found in Google key file!")
        return key_data[field_name]

    @staticmethod
    def pubsub_topic_exists(topic_id):

        # Check to see if the reporting Pub/Sub topic exists
        cmd = "gcloud beta pubsub topics list --format=json"
        out, err = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True).communicate()

        if len(err):
            logging.error("Cannot verify if the pubsub topic '%s' exists. The following error appeared: %s" % (topic_id, err))
            raise GoogleCloudHelperError("Cannot verify if pubsub topic exists. Please check the above error message.")

        topics = json.loads(out)
        for topic in topics:
            if topic["topicId"] == topic_id:
                return True

        return False

    @staticmethod
    def get_bucket_from_path(path):
        if not path.startswith("gs://"):
            logging.error("Cannot extract bucket from path '%s'. Invalid GoogleStorage path!")
            raise GoogleCloudHelperError("Attempt to get bucket from invalid GoogleStorage path. GS paths must begin with 'gs://'")
        return "/".join(path.split("/")[0:3]) + "/"

    @staticmethod
    def gs_path_exists(gs_path):
        # Check if path exists on google bucket storage
        cmd = "gsutil ls %s" % gs_path
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
        out, err = proc.communicate()
        return len(err) == 0

    @staticmethod
    def mb(gs_bucket, project, region):
        cmd = "gsutil mb -p %s -c regional -l %s %s" % (project, region, gs_bucket)
        GoogleCloudHelper.run_cmd(cmd, "Unable to make bucket '%s'!" % gs_bucket)

    @staticmethod
    def get_optimal_instance_type(nr_cpus, mem, zone, is_preemptible=False):

        # Obtaining prices from Google Cloud Platform
        prices = GoogleCloudHelper.get_prices()

        # Defining instance types to mem/cpu ratios
        ratio = dict()
        ratio["highcpu"] = 1.80 / 2
        ratio["standard"] = 7.50 / 2
        ratio["highmem"] = 13.00 / 2

        # Identifying needed predefined instance type
        if nr_cpus == 1:
            instance_type = "standard"
        else:
            ratio_mem_cpu = mem * 1.0 / nr_cpus
            if ratio_mem_cpu <= ratio["highcpu"]:
                instance_type = "highcpu"
            elif ratio_mem_cpu <= ratio["standard"]:
                instance_type = "standard"
            else:
                instance_type = "highmem"

        # Obtain available machine type in the current zone
        machine_types = GoogleCloudHelper.get_machine_types(zone)

        # Initializing predefined instance data
        predef_inst = {}

        # Obtain the machine type that has the closes number of CPUs
        predef_inst["nr_cpus"] = sys.maxsize
        for machine_type in machine_types:

            # Skip instances that are not of the same type
            if instance_type not in machine_type["name"]:
                continue

            # Select the instance if its number of vCPUs is closer to the required nr_cpus
            if machine_type["guestCpus"] >= nr_cpus and machine_type["guestCpus"] < predef_inst["nr_cpus"]:
                predef_inst["nr_cpus"] = machine_type["guestCpus"]
                predef_inst["mem"] = machine_type["memoryMb"] / 1024
                predef_inst["type_name"] = machine_type["name"]

        # Obtaining the price of the predefined instance
        region = GoogleCloudHelper.get_region(zone)
        if is_preemptible:
            predef_inst["price"] = prices["CP-COMPUTEENGINE-VMIMAGE-%s-PREEMPTIBLE" % predef_inst["type_name"].upper()][region]
        else:
            predef_inst["price"] = prices["CP-COMPUTEENGINE-VMIMAGE-%s" % predef_inst["type_name"].upper()][region]

        # Initializing custom instance data
        custom_inst = {}

        # Computing the number of cpus for a possible custom machine and making sure it's an even number or 1.
        custom_inst["nr_cpus"] = 1 if nr_cpus == 1 else nr_cpus + nr_cpus%2

        # Making sure the memory value is not under HIGHCPU and not over HIGHMEM
        if nr_cpus != 1:
            mem = max(ratio["highcpu"] * custom_inst["nr_cpus"], mem)
            mem = min(ratio["highmem"] * custom_inst["nr_cpus"], mem)
        else:
            mem = max(1, mem)
            mem = min(6, mem)

        # Computing the ceil of the current memory
        custom_inst["mem"] = int(math.ceil(mem))

        # Generating custom instance name
        custom_inst["type_name"] = "custom-%d-%d" % (custom_inst["nr_cpus"], custom_inst["mem"])

        # Computing the price of a custom instance
        if is_preemptible:
            custom_price_cpu = prices["CP-COMPUTEENGINE-CUSTOM-VM-CORE-PREEMPTIBLE"][region]
            custom_price_mem = prices["CP-COMPUTEENGINE-CUSTOM-VM-RAM-PREEMPTIBLE"][region]
        else:
            custom_price_cpu = prices["CP-COMPUTEENGINE-CUSTOM-VM-CORE"][region]
            custom_price_mem = prices["CP-COMPUTEENGINE-CUSTOM-VM-RAM"][region]
        custom_inst["price"] = custom_price_cpu * custom_inst["nr_cpus"] + custom_price_mem * custom_inst["mem"]

        # Determine which is cheapest and return
        if predef_inst["price"] <= custom_inst["price"]:
            nr_cpus = predef_inst["nr_cpus"]
            mem = predef_inst["mem"]
            instance_type = predef_inst["type_name"]

        else:
            nr_cpus = custom_inst["nr_cpus"]
            mem = custom_inst["mem"]
            instance_type = custom_inst["type_name"]

        return nr_cpus, mem, instance_type

    @staticmethod
    def get_instance_price(nr_cpus, mem, disk_space, instance_type, zone, is_preemptible=False, is_boot_disk_ssd=False, nr_local_ssd=0):

        prices = GoogleCloudHelper.get_prices()
        region = GoogleCloudHelper.get_region(zone)
        is_custom = instance_type.startswith('custom')
        price = 0

        # Get price of CPUs, mem for custom instance
        if is_custom:
            cpu_price_key = "CP-COMPUTEENGINE-CUSTOM-VM-CORE"
            mem_price_key = "CP-COMPUTEENGINE-CUSTOM-VM-RAM"
            if is_preemptible:
                cpu_price_key += "-PREEMPTIBLE"
                mem_price_key += "-PREEMPTIBLE"
            price += prices[cpu_price_key][region]*nr_cpus + prices[mem_price_key][region]*mem

        # Get price of CPUs, mem for standard instance
        else:
            price_key = "CP-COMPUTEENGINE-VMIMAGE-%s" % instance_type.upper()
            if is_preemptible:
                price_key = "CP-COMPUTEENGINE-VMIMAGE-%s-PREEMPTIBLE" % instance_type.upper()
            price += prices[price_key][region]

        # Get price of the instance's disk
        if is_boot_disk_ssd:
            price += prices["CP-COMPUTEENGINE-STORAGE-PD-SSD"][region] * disk_space / 730.0
        else:
            price += prices["CP-COMPUTEENGINE-STORAGE-PD-CAPACITY"][region] * disk_space / 730.0

        # Get price of local SSDs (if present)
        if nr_local_ssd:
            if is_preemptible:
                price += nr_local_ssd * prices["CP-COMPUTEENGINE-LOCAL-SSD-PREEMPTIBLE"][region] * 375
            else:
                price += nr_local_ssd * prices["CP-COMPUTEENGINE-LOCAL-SSD"][region] * 375

        return price




