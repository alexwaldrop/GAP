import logging
import threading
import math
import os
import time
import subprocess as sp
from collections import OrderedDict

from GoogleProcess import GoogleProcess
from GoogleException import GoogleException

class Instance(object):

    # Instance status values available between threads
    OFF         = 0     # Destroyed or not allocated on the cloud
    AVAILABLE   = 1     # Available for running processes
    BUSY        = 2     # Instance actions, such as create and destroy are running
    DEAD        = 3     # Instance is shutting down, as a DEAD signal was received

    MAX_STATUS  = 3     # Maximum status value possible

    def __init__(self, name, nr_cpus, mem, **kwargs):

        # Setting variables
        self.name               = name
        self.nr_cpus            = nr_cpus
        self.mem                = mem

        self.instance_type      = kwargs.get("instance_type",    "n1-standard-1")
        self.is_server          = kwargs.get("is_server",        False)
        self.boot_disk_size     = kwargs.get("boot_disk_size",   15)
        self.is_boot_disk_ssd   = kwargs.get("is_boot_disk_ssd", False)
        self.is_preemptible     = kwargs.get("is_preemptible",   False)
        self.zone               = kwargs.get("zone",             "us-east1-b")
        self.server_ip          = kwargs.get("server_ip",        "10.240.1.0")
        self.server_port        = kwargs.get("server_port",      "27708")
        self.nr_local_ssd       = kwargs.get("nr_local_ssd",     0)
        self.start_up_script    = kwargs.get("start_up_script",  None)
        self.shutdown_script    = kwargs.get("shutdown_script",  None)
        self.instances          = kwargs.get("instances",        {})
        self.main_server        = kwargs.get("main_server",      None)

        self.attached_disks     = list()
        self.processes          = OrderedDict()

        self.is_resetting       = False
        self.reset_count        = 0
        self.MAX_RESET_COUNT    = 5

        # Setting the instance status
        self.status_lock        = threading.Lock()
        self.status             = Instance.OFF

        self.google_id          = None

    def create(self):

        self.set_status(Instance.BUSY)

        logging.info("(%s) Process 'create' started!" % self.name)

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
        args.append("gap-412@davelab-gcloud.iam.gserviceaccount.com=\"https://www.googleapis.com/auth/cloud-platform\"")

        args.append("--image")
        args.append("davelab-image")

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

        metadata_args.append("server-ip=%s" % self.server_ip)

        metadata_args.append("server-port=%s" % self.server_port)

        if self.main_server is None:
            metadata_args.append("is-child=0")
        else:
            metadata_args.append("is-child=1")
            metadata_args.append("main-server=%s" % self.main_server)

        args.append("--metadata")
        args.append(",".join(metadata_args))

        args.append("--zone")
        args.append(self.zone)

        with open(os.devnull, "w") as devnull:
            self.processes["create"] = GoogleProcess(" ".join(args), stdout=devnull, stderr=devnull, shell=True)

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

        with open(os.devnull, "w") as devnull:
            self.processes["destroy"] = GoogleProcess(" ".join(args), stdout=devnull, stderr=devnull, shell=True)

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

    def got_preempted(self, instance_id=None):

        # Nornmal instances cannot be preempted
        if not self.is_preemptible:
            return False

        # If DEAD signal was received
        if self.get_status() == Instance.DEAD:
            return True

        if instance_id is None:
            # Check if google_id was obtained
            cycle_count = 0
            while self.google_id is None:
                cycle_count += 1
                time.sleep(2)

                if cycle_count == 30:
                    logging.error("(%s) Cannot find the instance preemptile status, because cannot obtain the google instance id!" % self.name)
                    raise GoogleException(self.name)

            instance_id = self.google_id

        cmd_filter = "jsonPayload.resource.id=%d AND jsonPayload.event_subtype=compute.instances.preempted" % instance_id

        cmd = "gcloud beta logging read \"%s\" --limit 1 --freshness=50d" % cmd_filter

        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)

        out, err = proc.communicate()

        if len(err) != 0:
            logging.error("(%s) Getting instance preemptible status failed with the following error: %s!" % (self.name, err))
            raise GoogleException(self.name)

        if len(out) == 0:
            return False
        else:
            logging.debug("(%s) According to Google Logging, instance has been preempted!" % self.name)
            return True

    def get_google_id(self):

        cmd = "gcloud compute instances describe %s --zone %s | grep \"id\"" % (self.name, self.zone)

        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)

        out, err = proc.communicate()

        if len(err) != 0:
            logging.error("(%s) Getting instance id failed with the following error: %s!" % (self.name, err))

        self.google_id = int(out.split(" ")[-1].strip("\n'"))

        logging.debug("(%s) Instance google ID obtained: %d." % (self.name, self.google_id))

    @staticmethod
    def get_type(nr_cpus, mem):

        # Treating special cases
        if nr_cpus == 1:
            if mem <= 0.6:
                return 1, 0.6, "f1-micro"
            if mem <= 1.7:
                return 1, 1.7, "g1-small"
            if mem <= 3.5:
                return 1, 3.5, "n1-standard-1"

        # Defining instance type to mem/cpu ratios
        ratio = dict()
        ratio["highcpu"] = 1.80 / 2
        ratio["standard"] = 7.50 / 2
        ratio["highmem"] = 13.00 / 2

        # Identifying needed instance type
        ratio_mem_cpu = mem * 1.0 / nr_cpus
        if ratio_mem_cpu <= ratio["highcpu"]:
            instance_type = "highcpu"
        elif ratio_mem_cpu <= ratio["standard"]:
            instance_type = "standard"
        else:
            instance_type = "highmem"

        # Converting the number of cpus to the closest upper power of 2
        nr_cpus = 2 ** math.ceil(math.log(nr_cpus, 2))

        # Computing the memory obtain on the instance
        mem = nr_cpus * ratio[instance_type]

        # Setting the instance type name
        inst_type_name = "n1-%s-%d" % (instance_type, nr_cpus)

        return nr_cpus, mem, inst_type_name

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

        # Exclude for loops out of the logging system (syntax interference)
        if "for " in command:
            log = False

        if log:
            log_cmd_all = " >>/data/logs/%s.log 2>&1 " % job_name
            log_cmd_stderr = " 2>>/data/logs/%s.log " % job_name

            command = command.replace("; ", log_cmd_all + "; ")
            command = command.replace(" && ", log_cmd_all + " && ")
            command = command.replace(" & ", log_cmd_all + " & ")
            command = command.replace(" | ", log_cmd_stderr + " | ")
            if " > " in command:
                command += " %s" % log_cmd_stderr
            else:
                command += " %s" % log_cmd_all

        cmd = "gcloud compute ssh gap@%s --command '%s' --zone %s" % (self.name, command, self.zone)

        logging.info("(%s) Process '%s' started!" % (self.name, job_name))
        logging.debug("(%s) Process '%s' has the following command:\n    %s" % (self.name, job_name, command))

        # Generating process arguments
        kwargs = dict()
        kwargs["instance_id"] = self.google_id
        kwargs["shell"] = True
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
                raise GoogleException(self.name)

            elif proc_name == "destroy":
                # Check if the instance is still present on the cloud
                cmd = 'gcloud compute instances list | grep "%s"' % self.name
                out, _ = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True).communicate()
                if len(out) != 0:
                    logging.info("(%s) Process '%s' failed!" % (self.name, proc_name))
                    raise GoogleException(self.name)

            else:
                # Check if the process failed on a preempted instance
                logging.debug("(%s) Process '%s' has failed on instance with id %s." % (self.name, proc_name, proc_obj.get_instance_id()))

                # Waiting for maximum 1 minute for the preemption to be logged or receive a DEAD signal
                preempted = False
                cycle_count = 1
                while cycle_count < 20:

                    if self.get_status() == Instance.DEAD:
                        preempted = True
                        break

                    if self.got_preempted(proc_obj.get_instance_id()):
                        preempted = True
                        break

                    time.sleep(6)
                    cycle_count += 1

                # Checking if the instance got preempted
                if preempted:
                    self.reset()
                else:
                    logging.info("(%s) Process '%s' failed!"  % (self.name, proc_name))
                    raise GoogleException(self.name)

        else:

            # Logging the process
            logging.info("(%s) Process '%s' complete!" % (self.name, proc_name))

            # Perform additional steps
            if proc_name == "create":
                # Obtain Google Instance ID
                self.get_google_id()

                # Waiting for instance to receive READY signal
                cycle_count = 0
                while self.get_status() != Instance.AVAILABLE:
                    cycle_count += 1
                    time.sleep(10)

                    if self.got_preempted():
                        self.reset()

                    if cycle_count > 30:
                        self.reset()

            elif proc_name == "destroy":
                self.set_status(Instance.OFF)

    def wait_all(self):

        for proc_name, proc_obj in self.processes.iteritems():
            self.wait_process(proc_name)