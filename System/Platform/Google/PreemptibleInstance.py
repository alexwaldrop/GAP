import logging
import subprocess as sp
import time

from System.Platform import Process
from System.Platform import Processor
from Instance import GoogleStandardProcessor

class GooglePreemptibleProcessor(GoogleStandardProcessor):

    def __init__(self, name, nr_cpus, mem, disk_space, **kwargs):
        # Call super constructor
        super(GooglePreemptibleProcessor,self).__init__(name, nr_cpus, mem, disk_space, **kwargs)

        # Indicates that instance is resettable
        self.is_preemptible = True

        # Attributes for handling instance resets
        self.max_resets = kwargs.pop("max_resets", 6)
        self.reset_count = 0

        # Stack for determining costs across resets
        self.cost_history = []
    
    def reset(self):
        # Resetting takes place just for preemptible instances
        if not self.is_preemptible:
            return

        # Reset as standard instance if preempted b/c runtime > 24 hours
        if self.start_time is not None and time.time() - self.start_time >= (3600 * 24):
            logging.warning("(%s) Instance failed! Preemptible runtime > 24 hrs. Resetting as standard instance." % self.name)
            self.is_preemptible = False

        # Incrementing the reset count and checking if it reached the threshold
        self.reset_count += 1
        if self.reset_count >= self.max_resets:
            logging.warning("(%s) Instance failed! Instance preempted and out of reset (num resets: %s). "
                            "Resetting as standard insance." % (self.name, self.max_resets))
            # Switch to non-preemptible instance
            self.is_preemptible = False

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

        # Remove commands that get run during configure_instance()
        for proc in ["configCRCMOD", "install_packages", "configureSSH", "restartSSH"]:
            if proc in self.processes:
                self.processes.pop(proc)

        # Identifying which process(es) need to be recalled
        commands_to_run = list()
        while len(self.processes):
            process_tuple = self.processes.popitem(last=False)
            commands_to_run.append((process_tuple[0], process_tuple[1]))

        # Recreating the instance
        self.create()

        # Rerunning all the commands
        if len(commands_to_run):
            while len(commands_to_run) != 0:
                proc_name, proc_obj = commands_to_run.pop(0)
                self.run(job_name=proc_name,
                         cmd=proc_obj.get_command(),
                         num_retries=proc_obj.get_num_retries(),
                         docker_image=proc_obj.get_docker_image(),
                         quiet_failure=proc_obj.is_quiet())
                self.wait_process(proc_name)

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

    def handle_failure(self, proc_name, proc_obj):

        # Determine if command can be retried
        can_retry   = False
        needs_reset = False

        logging.debug("(%s) Handling failure for proc '%s'. Curr status: %s" % (self.name, proc_name, self.get_status()))

        # Raise error if processor is locked
        if self.is_locked() and proc_name != "destroy":
            self.raise_error(proc_name, proc_obj)

        # Re-run any command (except create) if instance is up and cmd can be retried
        elif self.get_status() == Processor.AVAILABLE:
            can_retry = proc_obj.get_num_retries() > 0 and proc_name != "create"

        # Re-run destroy command if instance is creating and cmd has enough retries
        elif self.get_status() == Processor.CREATING:
            can_retry = proc_name == "destroy" and proc_obj.get_num_retries() > 0

        elif self.get_status() == Processor.DESTROYING:
            # Re-run destroy command

            # Instance is destroying itself and we know why (we killed it programmatically)
            if proc_name == "destroy" and proc_obj.get_num_retries() > 0:
                can_retry = True

            # Reset instance and re-run command if it failed and we're not sure why the instance is destroying itself (e.g. preemption)
            elif "destroy" not in self.processes and proc_name not in ["create", "destroy"]:
                needs_reset = True

        elif self.get_status() == Processor.OFF:
            # Don't do anythying if destroy failed but instance doesn't actually exist anymore
            if proc_name == "destroy":
                return

            # Handle cases where we have no idea why the instance doesn't currently exist (e.g. preemption, manual deletion)
            # Retry if 'create' command failed and instance doesn't exist
            if "destroy" not in self.processes and proc_name == "create" and proc_obj.get_num_retries() > 0:
                can_retry = True

            # Reset instance and re-run command if command failed and no sure why instance doesn't exist (e.g. preemption, gets manually deleted)
            elif "destroy" not in self.processes:
                needs_reset = True


        # Reset instance if its been destroyed/disappeared unexpectedly (i.e. preemption)
        if needs_reset and self.is_preemptible:
            logging.warning("(%s) Instance preempted! Resetting..." % self.name)
            self.reset()

        # Retry start/destroy command
        elif can_retry and proc_name in ["create", "destroy"]:
            logging.warning("(%s) Process '%s' failed but we still got %s retries left. Re-running command!" % (self.name, proc_name, proc_obj.get_num_retries()))
            self.processes[proc_name] = Process(proc_obj.get_command(),
                                                cmd=proc_obj.get_command(),
                                                stdout=sp.PIPE,
                                                stderr=sp.PIPE,
                                                shell=True,
                                                num_retries=proc_obj.get_num_retries() - 1)
        # Retry 'run' command
        elif can_retry:
            logging.warning("(%s) Process '%s' failed but we still got %s retries left. Re-running command!" % (
            self.name, proc_name, proc_obj.get_num_retries()))
            self.run(job_name=proc_name,
                     cmd=proc_obj.get_command(),
                     num_retries=proc_obj.get_num_retries() - 1,
                     docker_image=proc_obj.get_docker_image(),
                     quiet_failure=proc_obj.is_quiet())

        # Raise error if command failed, has no retries, and wasn't caused by preemption
        else:
            self.raise_error(proc_name, proc_obj)

    def wait_until_ready(self):
        # Wait until startup-script has completed on instance
        # This signifies that the instance has initialized ssh and the instance environment is finalized
        cycle_count = 1
        # Waiting for 10 minutes for status to change from creating
        while cycle_count < 300 and self.get_status() == Processor.CREATING and not self.is_locked():
            time.sleep(2)
            cycle_count += 1

        if self.is_locked():
            logging.debug("(%s) Instance locked while waiting for creation!" % self.name)
            raise RuntimeError("(%s) Instance locked while waiting for creation!" % self.name)

        # Run any commands necessary to make instance ready to run if startup script finished
        elif self.get_status() == Processor.AVAILABLE:
            logging.debug("(%s) Waiting for additional startup commands to run..." % self.name)
            self.configure_instance()

        # Handle what happends if processor is being/has been destroyed
        elif self.get_status() in [Processor.DESTROYING, Processor.OFF]:
            if "destroy" in self.processes:
                logging.debug("(%s) Instance destroyed while waiting for creation!" % self.name)
                raise RuntimeError("(%s) Instance destroyed while waiting for creation!" % self.name)

            # Instance was preempted so reset it
            else:
                logging.warning("(%s) Instance preempted! Resetting..." % self.name)
                self.reset()

        # Reset if instance not initialized within the alloted timeframe
        else:
            logging.debug("(%s) Create took more than 20 minutes! Resetting instance!" % self.name)
            self.reset()
