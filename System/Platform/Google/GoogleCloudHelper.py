import logging
import subprocess as sp
import json
import random
import requests
import base64
import os

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
        return None
