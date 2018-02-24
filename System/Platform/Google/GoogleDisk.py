import logging
import subprocess as sp


class GoogleDisk(object):

    def __init__(self, name, zone, size=10, is_ssd=False):

        # Initialize the disk variables
        self.name   = name.replace("_", "-").replace(".", "-").lower()
        self.zone   = zone
        self.size   = size
        self.type   = "pd-ssd" if is_ssd else "pd-standard"

        # Initialize flag that specifies if the disk has been created
        self.created = False

    def get_name(self):
        return self.name

    def create(self):

        logging.info("Creating '%s' disk '%s' of size %sGB." % (self.type, self.name, self.size))

        # Generate creation command
        cmd = "gcloud --no-user-output-enabled compute disks create %s --zone %s --size=%s --type=%s" \
              % (self.name, self.zone, self.size, self.type)

        # Generate process and wait for the run
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
        out, err = proc.communicate()

        # Report if any errors took place
        if err:
            logging.error("Could not create disk '%s'. The following error appeared: %s" % (self.name, err))
            raise RuntimeError("Disk creation failed!")

        self.created = True

    def destroy(self):

        # Skip if no disk has been created anyways
        if not self.created:
            return

        logging.info("Destroying disk '%s'." % self.name)

        # Generate deletion command
        cmd = "gcloud --quiet --no-user-output-enabled compute disks delete %s --zone %s" % (self.name, self.zone)

        # Generate process and wait for the run
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
        out, err = proc.communicate()

        # Report if any errors took place
        if err:
            logging.error("Could not delete disk '%s'. The following error appeared: %s" % (self.name, err))
            raise RuntimeError("Disk deletion failed!")
