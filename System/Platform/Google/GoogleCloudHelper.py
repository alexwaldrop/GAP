import logging
import subprocess as sp
import json
import random

class GoogleCloudHelperError(Exception):
    pass

class GoogleCloudHelper:

    @staticmethod
    def run_cmd(cmd, err_msg=None):

        # Running and waiting for the command
        proc = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
        out, err = proc.communicate()

        # Check if any error has appeared
        if len(err) != 0 and "error" in err.lower():
            logging.error("GoogleCloudHelper could not run the following command:\n" % cmd)
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