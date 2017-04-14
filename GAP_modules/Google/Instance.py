import logging
import threading
import math
import os
import time
import subprocess as sp
import requests
from collections import OrderedDict

from GAP_modules.Google import GoogleProcess
from GAP_modules.Google import GoogleException

class Instance(object):

    # Instance status values available between threads
    OFF         = 0     # Destroyed or not allocated on the cloud
    AVAILABLE   = 1     # Available for running processes
    BUSY        = 2     # Instance actions, such as create and destroy are running
    DEAD        = 3     # Instance is shutting down, as a DEAD signal was received

    MAX_STATUS  = 3     # Maximum status value possible

    def __init__(self, config, name, **kwargs):

        self.config = config
        self.name = name

        # Setting constants
        self.MAX_NR_CPUS        = self.config["platform"]["max_nr_cpus"]
        self.MAX_MEM            = self.config["platform"]["max_mem"]
        self.MAX_RESET_COUNT    = self.config["platform"]["max_reset"]

        # Setting variables
        self.nr_cpus            = kwargs.get("nr_cpus",          self.MAX_NR_CPUS)
        self.mem                = kwargs.get("mem",              self.MAX_MEM)
        self.is_server          = kwargs.get("is_server",        False)
        self.boot_disk_size     = kwargs.get("boot_disk_size",   50)
        self.is_boot_disk_ssd   = kwargs.get("is_boot_disk_ssd", False)
        self.is_preemptible     = kwargs.get("is_preemptible",   False)
        self.zone               = kwargs.get("zone",             "us-east1-b")
        self.ready_topic        = kwargs.get("ready_topic",      "ready")
        self.nr_local_ssd       = kwargs.get("nr_local_ssd",     0)
        self.start_up_script    = kwargs.get("start_up_script",  None)
        self.shutdown_script    = kwargs.get("shutdown_script",  None)
        self.instances          = kwargs.get("instances",        {})
        self.main_server        = kwargs.get("main_server",      None)
        self.instance_type      = kwargs.get("instance_type",    self.get_inst_type())

        self.attached_disks     = list()
        self.processes          = OrderedDict()

        self.is_resetting       = False
        self.reset_count        = 0

        # Setting the instance status
        self.status_lock        = threading.Lock()
        self.status             = Instance.OFF

        self.google_id          = None

    def create(self):

        self.set_status(Instance.BUSY)

        logging.info("(%s) Process 'create' started!" % self.name)
        logging.debug("(%s) Instance type is %s." % (self.name, self.instance_type))

        args = list()

        args.append("gcloud compute instances create %s" % self.name)

        args.append("--boot-disk-size")
        if self.boot_disk_size >= 1024:
            args.append("%dTB" % int(self.boot_disk_size/1024))
        else:
            args.append("%dGB" % int(self.boot_disk_size))

        args.append("--boot-disk-type")
        if self.is_boot_disk_ssd:
            args.append("pd-ssd")
        else:
            args.append("pd-standard")

        args.extend(["--local-ssd interface=scsi" for _ in xrange(self.nr_local_ssd)])

        args.append("--scopes")
        args.append("cloud-platform")

        args.append("--service-account")
        args.append("gap-412@davelab-gcloud.iam.gserviceaccount.com")

        args.append("--image")
        args.append("davelab-image")

        if "custom" in self.instance_type:
            args.append("--custom-cpu")
            args.append(str(self.nr_cpus))

            args.append("--custom-memory")
            args.append(str(self.mem))
        else:
            args.append("--machine-type")
            args.append(self.instance_type)

        if self.is_preemptible:
            args.append("--preemptible")

        metadata_args = list()
        if self.start_up_script is not None:
            metadata_args.append("startup-script-url=gs://davelab_data/scripts/%s" % self.start_up_script)
        else:
            metadata_args.append("startup-script-url=gs://davelab_data/scripts/startup.sh")

        if self.shutdown_script is not None:
            metadata_args.append("shutdown-script-url=gs://davelab_data/scripts/%s" % self.shutdown_script)
        else:
            metadata_args.append("shutdown-script-url=gs://davelab_data/scripts/shutdown.sh")

        metadata_args.append("is-nfs-server=%d" % self.is_server)

        metadata_args.append("ready-topic=%s" % self.ready_topic)

        if self.main_server is None:
            metadata_args.append("is-child=0")
        else:
            metadata_args.append("is-child=1")
            metadata_args.append("main-server=%s" % self.main_server)

        args.append("--metadata")
        args.append(",".join(metadata_args))

        args.append("--zone")
        args.append(self.zone)

        self.processes["create"] = GoogleProcess(" ".join(args), stdout=sp.PIPE, stderr=sp.PIPE, shell=True)

    def destroy(self):

        if self.get_status() == Instance.OFF:
            return

        self.set_status(Instance.BUSY)

        logging.info("(%s) Process 'destroy' started!" % self.name)

        args = list()

        args.append("gcloud compute instances delete %s" % self.name)

        args.append("--zone")
        args.append(self.zone)

        # Provide input to the command
        args[0:0] = ["yes", "2>/dev/null", "|"]

        self.processes["destroy"] = GoogleProcess(" ".join(args), stdout=sp.PIPE, stderr=sp.PIPE, shell=True)

    def reset(self):

        # Resetting takes place just for preemptible instances
        if not self.is_preemptible:
            return

        # Incrementing the reset count and checking if it reached the threshold
        self.reset_count += 1
        if self.reset_count >= self.MAX_RESET_COUNT:
            raise GoogleException(self.name)

        # Blocking other activities
        self.set_status(Instance.BUSY)
        self.is_resetting = True

        # Destroying the instance
        self.destroy()
        self.wait_process("destroy")

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
        self.wait_process("create")

        # Rerunning all the commands
        if len(commands_to_run):
            while len(commands_to_run) != 0:
                proc_name, proc_cmd = commands_to_run.pop(0)
                self.processes[proc_name] = GoogleProcess(proc_cmd, instance_id=self.google_id, shell=True)
                self.wait_process(proc_name)

        # Set as done resetting
        self.is_resetting = False

    def set_status(self, new_status):

        if new_status > Instance.MAX_STATUS or new_status < 0:
            logging.debug("(%s) Status level %d not available!" % (self.name, new_status))
            raise GoogleException(self.name)

        with self.status_lock:
            self.status = new_status

    def get_status(self):

        with self.status_lock:
            return self.status

    def get_google_id(self):

        cmd = "gcloud compute instances describe %s --zone %s | grep \"id\"" % (self.name, self.zone)

        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)

        out, err = proc.communicate()

        if len(err) != 0:
            logging.error("(%s) Getting instance id failed with the following error: %s!" % (self.name, err))

        self.google_id = int(out.split(" ")[-1].strip("\n'"))

        logging.debug("(%s) Instance google ID obtained: %d." % (self.name, self.google_id))

    def get_inst_type(self):

        # Making sure the values are not higher than possibly available
        if self.nr_cpus > self.MAX_NR_CPUS:
            logging.error("(%s) Cannot provision an instance with %d vCPUs. Maximum is %d vCPUs." % (self.name, self.nr_cpus, self.MAX_NR_CPUS))
            raise GoogleException(self.name)
        if self.mem > self.MAX_MEM:
            logging.error("(%s) Cannot provision an instance with %d GB RAM. Maximum is %d GB RAM." % (self.name, self.mem, self.MAX_MEM))
            raise GoogleException(self.name)

        # Obtaining prices from Google Cloud Platform
        try:
            price_json_url = "https://cloudpricingcalculator.appspot.com/static/data/pricelist.json"

            # Disabling low levels of logging from module requests
            logging.getLogger("requests").setLevel(logging.WARNING)

            prices = requests.get(price_json_url).json()["gcp_price_list"]
        except BaseException as e:
            if e.message != "":
                logging.error("Could not obtain instance prices. The following error appeared: %s." % e.message)
            raise

        # Defining instance types to mem/cpu ratios
        ratio = dict()
        ratio["highcpu"] = 1.80 / 2
        ratio["standard"] = 7.50 / 2
        ratio["highmem"] = 13.00 / 2

        # Identifying needed predefined instance type
        ratio_mem_cpu = self.mem * 1.0 / self.nr_cpus
        if ratio_mem_cpu <= ratio["highcpu"]:
            instance_type = "highcpu"
        elif ratio_mem_cpu <= ratio["standard"]:
            instance_type = "standard"
        else:
            instance_type = "highmem"

        # Initializing predefined instance data
        predef_inst = {}

        # Converting the number of cpus to the closest upper power of 2
        predef_inst["nr_cpus"] = 2 ** int(math.ceil(math.log(self.nr_cpus, 2)))

        # Computing the memory obtain on the instance
        predef_inst["mem"] = predef_inst["nr_cpus"] * ratio[instance_type]

        # Setting the instance type name
        predef_inst["type_name"] = "n1-%s-%d" % (instance_type, predef_inst["nr_cpus"])

        # Obtaining the price of the predefined instance
        if self.is_preemptible:
            predef_inst["price"] = prices["CP-COMPUTEENGINE-VMIMAGE-%s-PREEMPTIBLE" % predef_inst["type_name"].upper()]["us"]
        else:
            predef_inst["price"] = prices["CP-COMPUTEENGINE-VMIMAGE-%s" % predef_inst["type_name"].upper()]["us"]

        # Initializing custom instance data
        custom_inst = {}

        # Computing the number of cpus for a possible custom machine and making sure it's an even number or 1.
        if self.nr_cpus != 1:
            custom_inst["nr_cpus"] = self.nr_cpus + self.nr_cpus % 2

        # Computing the memory as integer value in GB
        custom_inst["mem"] = int(math.ceil(self.mem))

        # Making sure the memory value is not under HIGHCPU and not over HIGHMEM
        custom_inst["mem"] = max(ratio["highcpu"] * custom_inst["nr_cpus"], custom_inst["mem"])
        custom_inst["mem"] = min(ratio["highmem"] * custom_inst["nr_cpus"], custom_inst["mem"])

        # Generating custom instance name
        custom_inst["type_name"] = "custom-%d-%d" % (custom_inst["nr_cpus"], custom_inst["mem"])

        # Computing the price of a custom instance
        if self.is_preemptible:
            custom_price_cpu = prices["CP-COMPUTEENGINE-CUSTOM-VM-CORE-PREEMPTIBLE"]["us"]
            custom_price_mem = prices["CP-COMPUTEENGINE-CUSTOM-VM-RAM-PREEMPTIBLE"]["us"]
        else:
            custom_price_cpu = prices["CP-COMPUTEENGINE-CUSTOM-VM-CORE"]["us"]
            custom_price_mem = prices["CP-COMPUTEENGINE-CUSTOM-VM-RAM"]["us"]
        custom_inst["price"] = custom_price_cpu * custom_inst["nr_cpus"] + custom_price_mem * custom_inst["mem"]

        if predef_inst["price"] <= custom_inst["price"]:
            self.nr_cpus = predef_inst["nr_cpus"]
            self.mem = predef_inst["mem"]
            return predef_inst["type_name"]
        else:
            self.nr_cpus = custom_inst["nr_cpus"]
            self.mem = custom_inst["mem"]
            return custom_inst["type_name"]

    def attach_disk(self, disk, read_only=True):

        args = list()

        args.append("gcloud compute instances attach-disk %s" % self.name)

        args.append("--disk")
        args.append(disk.name)

        args.append("--mode")
        if read_only:
            args.append("ro")
        else:
            args.append("rw")

        args.append("--zone")
        args.append(self.zone)

        self.attached_disks.append(disk)

        with open(os.devnull, "w") as devnull:
            self.processes["attach_disk_%s" % disk.name] = GoogleProcess(" ".join(args), instance_id=self.google_id,
                                                                         stdout=devnull, stderr=devnull, shell=True)

    def detach_disk(self, disk):

        args = list()

        args.append("gcloud compute instances detach-disk %s" % self.name)

        args.append("--disk")
        args.append(disk.name)

        args.append("--zone")
        args.append(self.zone)

        self.attached_disks.remove(disk)

        with open(os.devnull, "w") as devnull:
            self.processes["detach_disk_%s" % disk.name] =  GoogleProcess(" ".join(args), instance_id = self.google_id,
                                                                          stdout=devnull, stderr=devnull, shell=True)

    def run_command(self, job_name, command, log=True, get_output=False, proc_wait=False):

        cycles_count = 0
        while self.get_status() != Instance.AVAILABLE:
            cycles_count += 0
            time.sleep(2)

            if cycles_count == 150:
                logging.error("(%s) Process '%s' waited too long!" % (self.name, job_name))
                raise GoogleException(self.name)

        # Checking if logging is required
        if "!LOG" in command:

            # Generating all the logging pipes
            log_cmd_null = " >>/dev/null 2>&1 "
            log_cmd_stdout = " >>/data/logs/%s.log " % job_name
            log_cmd_stderr = " 2>>/data/logs/%s.log " % job_name
            log_cmd_all = " >>/data/logs/%s.log 2>&1 " % job_name

            # Replacing the placeholders with the logging pipes
            command = command.replace("!LOG0!", log_cmd_null)
            command = command.replace("!LOG1!", log_cmd_stdout)
            command = command.replace("!LOG2!", log_cmd_stderr)
            command = command.replace("!LOG3!", log_cmd_all)

        cmd = "gcloud compute ssh gap@%s --command '%s' --zone %s" % (self.name, command, self.zone)

        if log:
            logging.info("(%s) Process '%s' started!" % (self.name, job_name))
            logging.debug("(%s) Process '%s' has the following command:\n    %s" % (self.name, job_name, command))

        # Generating process arguments
        kwargs = dict()
        kwargs["instance_id"] = self.google_id
        kwargs["shell"] = True
        kwargs["log"] = log
        if get_output:
            kwargs["stdout"] = sp.PIPE
            kwargs["stderr"] = sp.PIPE

        self.processes[job_name] = GoogleProcess(cmd, **kwargs)

        if get_output or proc_wait:
            self.wait_process(job_name)

            if get_output:
                return self.processes[job_name].communicate()

    def is_alive(self):

        if self.is_resetting:
            return False

        if self.get_status() != Instance.AVAILABLE:
            return False

        cmd = "gcloud compute ssh --ssh-flag=\"-o ConnectTimeout=10\" gap@%s --command 'echo'" % self.name

        with open(os.devnull, "w") as devnull:
            proc = sp.Popen(cmd, stdout=devnull, stderr=devnull, shell=True)

        if proc.wait() == 0:
            return True

        return False

    def poll_process(self, proc_name):

        if self.is_resetting:
            return False

        if self.get_status() != Instance.AVAILABLE:
            return False

        if proc_name not in self.processes:
            return False

        return self.processes[proc_name].is_done()

    def wait_process(self, proc_name):

        proc_obj = self.processes[proc_name]

        if proc_obj.complete:
            return

        # Waiting for process to finish
        ret_code = proc_obj.wait()

        # Process is complete!
        proc_obj.complete = True

        # Checking if process failed
        if ret_code != 0:

            if proc_name == "create":
                logging.info("(%s) Process '%s' failed!" % (self.name, proc_name))
                out, err = proc_obj.communicate()
                logging.info("(%s) The following error was received: \n  %s\n%s" % (self.name, out, err))
                raise GoogleException(self.name)

            elif proc_name == "destroy":
                # Check if the instance is still present on the cloud
                cmd = 'gcloud compute instances list | grep "%s"' % self.name
                out, _ = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True).communicate()
                if len(out) != 0:
                    logging.info("(%s) Process '%s' failed!" % (self.name, proc_name))
                    out, err = proc_obj.communicate()
                    logging.info("(%s) The following error was received: \n  %s\n%s" % (self.name, out, err))
                    raise GoogleException(self.name)

            else:
                # Check if the process failed on a preempted instance
                if proc_obj.log:
                    logging.debug("(%s) Process '%s' has failed on instance with id %s." % (self.name, proc_name, proc_obj.get_instance_id()))

                # Waiting for maximum 1 minute for the preemption to be logged or receive a DEAD signal
                preempted = False
                cycle_count = 1
                # Waiting 30 minutes for the instance to be reported as preempted
                while cycle_count < 900:

                    if self.get_status() == Instance.DEAD:
                        preempted = True
                        break

                    time.sleep(2)
                    cycle_count += 1

                # Checking if the instance got preempted
                if preempted:
                    self.reset()
                else:
                    if proc_obj.log:
                        logging.error("(%s) Process '%s' failed!"  % (self.name, proc_name))
                    raise GoogleException(self.name)

        else:

            # Logging the process
            if proc_obj.log:
                logging.info("(%s) Process '%s' complete!" % (self.name, proc_name))

            # Perform additional steps
            if proc_name == "create":
                # Obtain Google Instance ID
                self.get_google_id()

                # Waiting for instance to receive READY signal
                preempted = False
                completed = False
                cycle_count = 1
                # Waiting 30 minutes for the instance to be preempted
                while cycle_count < 900:

                    if self.get_status() == Instance.AVAILABLE:
                        completed = True
                        break
                    elif self.is_preemptible and self.get_status() == Instance.DEAD:
                        preempted = True
                        break

                    time.sleep(2)
                    cycle_count += 1

                if preempted:
                    self.reset()
                elif not completed:
                    if proc_obj.log:
                        logging.error("(%s) Could not create instance!" % self.name)
                    raise GoogleException(self.name)

            elif proc_name == "destroy":
                self.set_status(Instance.OFF)

    def wait_all(self):

        for proc_name, proc_obj in self.processes.iteritems():
            self.wait_process(proc_name)