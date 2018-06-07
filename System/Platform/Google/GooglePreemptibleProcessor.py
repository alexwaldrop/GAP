import logging
import subprocess as sp
import time
import json
import copy

from GoogleStandardProcessor import GoogleStandardProcessor
from GoogleCloudHelper import GoogleCloudHelper

class GooglePreemptibleProcessor(GoogleStandardProcessor):

    # Instance status values available between threads
    OFF         = 0     # Destroyed or not allocated on the cloud
    AVAILABLE   = 1     # Available for running processes
    BUSY        = 2     # Instance actions, such as create and destroy are running
    DEAD        = 3     # Instance is shutting down, as a DEAD signal was received
    MAX_STATUS  = 3     # Maximum status value possible

    def __init__(self, name, nr_cpus, mem, disk_space, **kwargs):
        # Call super constructor
        super(GooglePreemptibleProcessor,self).__init__(name, nr_cpus, mem, disk_space, **kwargs)

        # Indicates that instance is resettable
        self.is_preemptible = True

        # Attributes for handling instance resets
        self.max_resets     = kwargs.pop("max_resets", 6)
        self.is_resetting   = False
        self.reset_count    = 0

        # Stack for determining costs across resets
        self.cost_history = []

    def reset(self):
        # Resetting takes place just for preemptible instances
        if not self.is_preemptible:
            return

        # Incrementing the reset count and checking if it reached the threshold
        self.reset_count += 1
        if self.reset_count >= self.max_resets:
            logging.error("(%s) Instance failed! Instance preempted but has reached threshold for number of resets (%s)." %
                          (self.name, self.max_resets))

            # Switch to non-preemptible instance
            self.is_preemptible = False

        # Blocking other activities
        self.is_resetting = True
        self.set_status(GooglePreemptibleProcessor.BUSY)

        # Create cost history record
        prev_price = self.price
        prev_start = self.start_time

        # Destroying the instance
        self.destroy()

        # Add record to cost history of last run
        self.cost_history.append((prev_price, prev_start, self.stop_time))

        # Removing old process(es)
        self.processes.pop("create", None)
        self.processes.pop("destroy", None)

        # Identifying which process(es) need to be recalled
        commands_to_run = list()
        while len(self.processes):
            process_tuple = self.processes.popitem(last=False)
            #commands_to_run.append( (process_tuple[0], process_tuple[1].get_command()) )
            commands_to_run.append((process_tuple[0], process_tuple[1]))
        # Recreating the instance
        self.create()

        # Reset status to busy
        self.set_status(GooglePreemptibleProcessor.BUSY)

        # Rerunning all the commands
        if len(commands_to_run):
            while len(commands_to_run) != 0:
                #proc_name, proc_cmd = commands_to_run.pop(0)
                proc_name, proc_obj = commands_to_run.pop(0)
                self.run(job_name=proc_name,
                         cmd=proc_obj.get_command(),
                         num_retries=proc_obj.get_num_retries(),
                         docker_image=proc_obj.get_docker_image(),
                         quiet_failure=proc_obj.is_quiet())
                self.wait_process(proc_name)

        # Set as done resetting
        self.is_resetting = False

        # Set as available
        self.set_status(GooglePreemptibleProcessor.AVAILABLE)

    def set_status(self, new_status, wait_for_reset=True):

        # Check if new_status is a valid status
        if new_status > GoogleStandardProcessor.MAX_STATUS or new_status < 0:
            logging.debug("(%s) Status level %d not available!" % (self.name, new_status))
            raise RuntimeError("Instance %s has failed!" % self.name)

        # Check if the instance should be reset
        if ( (new_status == GooglePreemptibleProcessor.DEAD or              # instance is being set to DEAD
                self.get_status() == GooglePreemptibleProcessor.DEAD) and   # instance was set as DEAD before
                wait_for_reset and                                          # set_status can wait for a reset if needed
                not self.is_resetting and                                   # do not reset if already resetting
                new_status != GooglePreemptibleProcessor.OFF):              # do not reset if the instance is set to OFF
            self.reset()

        # Updates instance status with threading.lock() to prevent race conditions
        with self.status_lock:
            self.status = new_status

    def is_fatal_error(self, proc_name, err_msg):
        # Check to see if program should exit due to error received

        # Check if 'destroy' process actually deleted the instance, in which case program can continue running
        if proc_name == "destroy" and not self.exists():
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

            # Waiting 3 minutes for the instance to be reported as preempted
            while cycle_count < 90:
                if GoogleCloudHelper.get_instance_status(self.name, self.zone) in ["TERMINATED", "STOPPING"]:
                    # Reset the instance upon preemption detection
                    logging.info("(%s) Instance preempted! Instance will be reset." % self.name)
                    self.set_status(GooglePreemptibleProcessor.DEAD, wait_for_reset=False)
                    self.reset()
                    return False

                # Wait 2 seconds per cycle
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

        # Waiting 20 minutes for the instance to finish running startup script
        while cycle_count < 600 and not ready:

            # Check to see if instance has been preempted during creation
            if GoogleCloudHelper.get_instance_status(self.name, self.zone) in ["TERMINATED", "STOPPING"]:
                # Reset if dead
                logging.info("(%s) Instance preempted! Instance will be reset." % self.name)
                self.set_status(GooglePreemptibleProcessor.DEAD, wait_for_reset=False)
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

        # Reset instance if not allocated in 20 minutes
        if not ready:
            logging.info("(%s) Instance failed! 'Create' Process took more than 20 minutes! "
                         "The instance will be reset!" % self.name)
            self.reset()

    def get_runtime(self):
        # Compute total runtime across all resets
        # Return 0 if instance hasn't started yet
        if self.start_time is None:
            return 0

        # Instance is still running so register runtime since last start/restart
        elif self.stop_time is None or self.stop_time < self.start_time:
            runtime = time.time() - self.start_time

        # Instance has been stopped
        else:
            runtime = self.stop_time - self.start_time

        # Add previous runtimes from restart history
        for record in self.cost_history:
            runtime += record[2] - record[1]
        return runtime

    def compute_cost(self):
        # Compute total cost across all resets
        cost = 0
        if self.start_time is None:
            return 0

        elif self.stop_time is None or self.stop_time < self.start_time:
            cost = (time.time() - self.start_time) * self.price

        else:
            cost = (self.stop_time - self.start_time) * self.price

        for record in self.cost_history:
            cost += (record[2]-record[1]) * record[0]

        return cost/3600
