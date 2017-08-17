import logging
import subprocess as sp
import time
import json

from GoogleStandardProcessor import GoogleStandardProcessor

class GooglePreemptibleProcessor(GoogleStandardProcessor):

    # Instance status values available between threads
    OFF         = 0     # Destroyed or not allocated on the cloud
    AVAILABLE   = 1     # Available for running processes
    BUSY        = 2     # Instance actions, such as create and destroy are running
    DEAD        = 3     # Instance is shutting down, as a DEAD signal was received
    MAX_STATUS  = 3     # Maximum status value possible

    def __init__(self, name, nr_cpus, mem, **kwargs):
        # Call super constructor
        super(GooglePreemptibleProcessor,self).__init__(name, nr_cpus, mem, **kwargs)

        # Indicates that instance is resettable
        self.is_preemptible = True

        # Attributes for handling instance resets
        self.max_resets     = kwargs.pop("max_resets", 6)
        self.is_resetting   = False
        self.reset_count    = 0

    def reset(self):
        # Resetting takes place just for preemptible instances
        if not self.is_preemptible:
            return

        # Incrementing the reset count and checking if it reached the threshold
        self.reset_count += 1
        if self.reset_count >= self.max_resets:
            logging.error("(%s) Instance failed! Instance preempted but has reached threshold for number of resets (%s)." %
                          self.name, self.max_resets)
            raise RuntimeError("Instance %s has failed!" % self.name)

        # Blocking other activities
        self.set_status(GooglePreemptibleProcessor.BUSY)
        self.is_resetting = True

        # Destroying the instance
        self.destroy()

        # Removing old process(es)
        self.processes.pop("create", None)
        self.processes.pop("destroy", None)

        # Identifying which process(es) need to be recalled
        commands_to_run = list()
        while len(self.processes):
            process_tuple = self.processes.popitem(last=False)
            commands_to_run.append( (process_tuple[0], process_tuple[1].get_command()) )

        # Recreating the instance
        self.create()

        # Rerunning all the commands
        if len(commands_to_run):
            while len(commands_to_run) != 0:
                proc_name, proc_cmd = commands_to_run.pop(0)

                self.run(proc_name, proc_cmd)
                self.wait_process(proc_name)

        # Set as done resetting
        self.is_resetting = False

    def is_fatal_error(self, proc_name, err_msg):
        # Check to see if program should exit due to error received
        if proc_name == "destroy":
            # Check if 'destroy' process actually deleted the instance, in which case program can continue running
            cmd = 'gcloud compute instances list | grep "%s"' % self.name
            out, _ = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True).communicate()
            if len(out) == 0:
                return False

        elif "- Internal Error" in err_msg:
            # Check to see if error was result of internal google error
            # Reset the instance if error was due to internal google error
            logging.warning("(%s) Instance will be reset! A Google Internal Error was received:\n%s" % (self.name, err_msg))
            self.reset()
            return False

        elif proc_name != "create":
            # Check to see if error was preemption or user error
            cycle_count = 1

            # determine if user error or preempted
            if "ERROR: (gcloud.compute.ssh)" not in err_msg:
                # exit program if ssh error (from preemption) not found in error message
                return True

            # Waiting 30 minutes for the instance to be reported as preempted
            while cycle_count < 900:
                if self.get_status() == GooglePreemptibleProcessor.DEAD:
                    # Reset the instance upon preemption detection
                    logging.info("(%s) Instance preempted! Instance will be reset.")
                    self.reset()
                    return False
                time.sleep(2)
                cycle_count += 1
        # Exit on any other kind of error
        return True

    def wait_until_ready(self):
        # Wait until startup-script has completed on instance
        # This signifies that the instance has initialized ssh and the instance environment is finalized

        logging.info("(%s) Waiting for instance startup-script completion..." % self.name)
        ready = False
        cycle_count = 1

        # Waiting 20 minutes for the instance to finish running
        while cycle_count < 600 and not ready:

            # Check to see if instance has been preempted during creation
            if self.get_status() == GooglePreemptibleProcessor.DEAD:
                # Reset if dead
                logging.info("(%s) Instance preempted! Instance will be reset." % self.name)
                self.reset()

                # Reset cycle count to begin process all over again
                cycle_count = 1

            # Check the syslog to see if it contains text indicating the startup has completed
            cmd = 'gcloud compute instances describe %s --format json --zone %s' % (self.name, self.zone)
            proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
            out, err = proc.communicate()

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

        if not ready:
            logging.error("(%s) Instance failed! 'Create' Process took more than 20 minutes!" % self.name)
            raise RuntimeError("Instance %s has failed!" % self.name)