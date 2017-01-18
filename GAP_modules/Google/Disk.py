import logging
import os

from GAP_modules.Google import GoogleProcess

class Disk():

    def __init__(self, name, size, **kwargs):

        # Setting variables
        self.name   = name
        self.size   = size

        self.is_SSD     = kwargs.get("is_SSD",      False)
        self.zone       = kwargs.get("zone",        "us-east1-b")
        self.with_image = kwargs.get("with_image",  False)

        self.processes  = {}

    def create(self):
    #def create(self, wait_proc=False):

        logging.info("(%s) Disk process 'create' started!" % self.name)

        args = ["gcloud compute disks create %s" % self.name]

        args.append("--size")
        if self.size >= 1024:
            args.append("%dTB" % int(self.size/1024))
        else:
            args.append("%dGB" % int(self.size))

        args.append("--type")
        if self.is_SSD:
            args.append("pd-ssd")
        else:
            args.append("pd-standard")

        args.append("--zone")
        args.append(self.zone)

        if self.with_image:
            args.append("--image")
            args.append("ubuntu-14-04")

        with open(os.devnull, "w") as devnull:
            self.processes["create"] = GoogleProcess(" ".join(args), stdout=devnull, stderr=devnull, shell=True)

    def destroy(self):

        logging.info("(%s) Disk process 'destroy' started!" % self.name)

        args = ["gcloud compute disks delete %s" % self.name]

        args.append("--zone")
        args.append(self.zone)

        # Provide input to the command
        args[0:0] = ["yes", "2>/dev/null", "|"]

        with open(os.devnull, "w") as devnull:
            self.processes["destroy"] = GoogleProcess(" ".join(args), stdout=devnull, stderr=devnull, shell=True)

    def wait_all(self):

        for proc_name, proc_obj in self.processes.iteritems():
            proc_obj.wait()

            # Logging if not logged yet
            if not proc_obj.logged:
                logging.info("(%s) Disk process '%s' complete!" % (self.name, proc_name))
                proc_obj.logged = True
