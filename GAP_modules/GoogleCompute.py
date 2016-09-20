import math
import os
import subprocess as sp
import threading
import time
from datetime import datetime

from GAP_interfaces import Main

class GoogleException(Exception):

    def __init__(self, instance_name=None):

        if instance_name is not None:
            print("[%s]Instance %s has failed!" % (datetime.now(), instance_name))


class GoogleProcess(sp.Popen):

    def __init__(self, args, **kwargs):

        super(GoogleProcess, self).__init__(args, **kwargs)

        self.complete = False

        if isinstance(args, list):
            self.command = " ".join(args)

        else:
            self.command = args

    def is_done(self):

        return self.poll() is not None

    def has_failed(self):

        ret_code = self.poll()
        return ret_code is not None and ret_code != 0


class InstanceStatus(object):

    # Not initialized
    NONE        = 0

    # Machine Operations
    CREATING    = 1
    DESTROYING  = 2
    RESETTING   = 3

    # Process statuses
    FAILED      = 4
    RUNNING     = 5

    # Not working
    IDLE        = 6

    def __init__(self):

        self.lock   = threading.Lock()
        self.status = InstanceStatus.NONE

    def set(self, new_status):

        with self.lock:

            if new_status <= InstanceStatus.DESTROYING and self.status == InstanceStatus.RESETTING:
                self.status = InstanceStatus.RESETTING
            else:
                self.status = new_status

    def get(self):

        with self.lock:
            return self.status


class Instance(object):

    def __init__(self, name, nr_cpus, mem, **kwargs):

        # Setting variables
        self.name               = name
        self.nr_cpus, self.mem, self.instance_type = GoogleCompute.get_instance_type(nr_cpus, mem)

        self.is_server          = kwargs.get("is_server",        False)
        self.boot_disk_size     = kwargs.get("boot_disk_size",   15)
        self.is_boot_disk_ssd   = kwargs.get("is_boot_disk_ssd", False)
        self.is_preemptible     = kwargs.get("is_preemptible",   False)
        self.zone               = kwargs.get("zone",             "us-east1-b")
        self.nr_local_ssd       = kwargs.get("nr_local_ssd",     0)
        self.start_up_script    = kwargs.get("start_up_script",  None)
        self.instances          = kwargs.get("instances",        {})

        self.attached_disks     = []
        self.processes          = {}

        # The instance status
        self.status             = InstanceStatus()

    def create(self):

        self.status.set(InstanceStatus.CREATING)

        print("[%s] --------Creating %s" % (datetime.now(), self.name))

        args = ["gcloud compute instances create %s" % self.name]

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

        args.extend(["--local-ssd ''" for _ in xrange(self.nr_local_ssd)])

        args.append("--scopes")
        args.append("gap-412@davelab-gcloud.iam.gserviceaccount.com=\"https://www.googleapis.com/auth/cloud-platform\"")

        args.append("--image")
        args.append("/davelab-gcloud/davelab-image")

        args.append("--machine-type")
        args.append(self.instance_type)

        if self.is_preemptible:
            args.append("--preemptible")

        if self.start_up_script is not None:
            args.append("--metadata")
            args.append("startup-script-url=gs://davelab_data/scripts/%s" % self.start_up_script)
        elif self.is_server:
            if self.nr_local_ssd == 0:
                args.append("--metadata")
                args.append("startup-script-url=gs://davelab_data/scripts/nfs.sh")
            elif self.nr_local_ssd == 1:
                args.append("--metadata")
                args.append("startup-script-url=gs://davelab_data/scripts/nfs_LocalSSD.sh")
            elif self.nr_local_ssd > 1:
                args.append("--metadata")
                args.append("startup-script-url=gs://davelab_data/scripts/nfs_LocalSSD_RAID.sh")
        else:
            if self.nr_local_ssd == 0:
                args.append("--metadata")
                args.append("startup-script-url=gs://davelab_data/scripts/default.sh")
            elif self.nr_local_ssd == 1:
                args.append("--metadata")
                args.append("startup-script-url=gs://davelab_data/scripts/LocalSSD.sh")
            elif self.nr_local_ssd > 1:
                args.append("--metadata")
                args.append("startup-script-url=gs://davelab_data/scripts/LocalSSD_RAID.sh")

        args.append("--zone")
        args.append(self.zone)

        with open(os.devnull, "w") as devnull:
            self.processes["create"] = GoogleProcess(" ".join(args), stdout=devnull, stderr=devnull, shell=True)

    def destroy(self):

        self.status.set(InstanceStatus.DESTROYING)

        print("[%s]--------Destroying %s" % (datetime.now(), self.name))

        args = ["gcloud compute instances delete %s" % self.name]

        args.append("--zone")
        args.append(self.zone)

        # Provide input to the command
        args[0:0] = ["yes", "2>/dev/null", "|"]

        with open(os.devnull, "w") as devnull:
            self.processes["destroy"] = GoogleProcess(" ".join(args), stdout=devnull, stderr=devnull, shell=True)

    def attach_disk(self, disk, read_only=True):

        args = ["gcloud compute instances attach-disk %s" % self.name]

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
            self.processes["attach_disk_%s" % disk.name] = GoogleProcess(" ".join(args), stdout=devnull, stderr=devnull, shell=True)

    def detach_disk(self, disk):

        args = ["gcloud compute instances detach-disk %s" % self.name]

        args.append("--disk")
        args.append(disk.name)

        args.append("--zone")
        args.append(self.zone)

        self.attached_disks.remove(disk)

        with open(os.devnull, "w") as devnull:
            self.processes["detach_disk_%s" % disk.name] =  GoogleProcess(" ".join(args), stdout=devnull, stderr=devnull, shell=True)

    def run_command(self, job_name, command, proc_wait=False):

        self.status.set(InstanceStatus.RUNNING)

        cmd = "gcloud compute ssh gap@%s --command '%s'" % (self.name, command)

        self.processes[job_name] = GoogleProcess(cmd, shell=True)

        if proc_wait:
            ret_code = self.processes[job_name].wait()

            if ret_code != 0:
                self.status.set(InstanceStatus.FAILED)
                raise GoogleException(self.name)

    def is_alive(self):

        cmd = "gcloud compute ssh --ssh-flag=\"-o ConnectTimeout=10\" gap@%s --command 'echo'" % self.name

        with open(os.devnull, "w") as devnull:
            proc = GoogleProcess(cmd, stdout=devnull, stderr=devnull, shell=True)

        if proc.wait() == 0:
            return True

        return False

    def poll_process(self, proc_name):

        if proc_name not in self.processes:
            print("[%s]-----Process %s not found!" % (datetime.now(), proc_name))
            return False

        return self.processes[proc_name].is_done()


class RegularInstance(Instance):

    def __init__(self, name, nr_cpus, mem, **kwargs):

        super(RegularInstance, self).__init__(name, nr_cpus, mem, **kwargs)

    def wait_process(self, proc_name):

        proc_obj = self.processes[proc_name]

        if proc_obj.complete:
            return

        # Waiting for process to finish
        ret_code = proc_obj.wait()

        # Checking if process failed
        if ret_code != 0:
            print("[%s]--------Process '%s' on instance '%s' failed!" % (datetime.now(), proc_name, self.name))
            self.status.set(InstanceStatus.FAILED)

            proc_obj.complete = True
            raise GoogleException(self.name)

        # Logging the process
        print("[%s]--------Process '%s' on instance '%s' complete!" % (datetime.now(), proc_name, self.name))

        # Changing status if needed
        if proc_name == "create":
            self.status.set(InstanceStatus.IDLE)
        elif proc_name == "destroy":
            self.status.set(InstanceStatus.NONE)

        # Mark as complete
        proc_obj.complete = True

    def wait_all(self):

        for proc_name, proc_obj in self.processes.iteritems():
            self.wait_process(proc_name)

        # All processes are finished
        self.status.set(InstanceStatus.IDLE)


class PreemptibleInstance(Instance):

    def __init__(self, name, nr_cpus, mem, **kwargs):

        super(PreemptibleInstance, self).__init__(name, nr_cpus, mem, **kwargs)

        self.split_id = int(name.split("-")[0].split("split")[-1])

        # Instance is available (used for resetting)
        self.available_event    = threading.Event()
        self.available_event.set()

        # Heart active event
        self.heart_event        = threading.Event()
        self.heart_event.clear()

        # Number of times the instance has not responded
        self.times_silent       = 0

        # Number of times same instance has been resetted
        self.reset_count        = 0

        # Heartbeat thread
        self.HEARTBEAT_INTERVAL = 5.0
        heart_thread            = threading.Timer(self.HEARTBEAT_INTERVAL, self.heartbeat)
        heart_thread.daemon     = True
        heart_thread.start()

    def destroy(self):

        # Stop the heart
        self.heart_event.clear()

        super(PreemptibleInstance, self).destroy()

    def start_heart(self):

        self.heart_event.set()

    def reset(self):
        self.status.set(InstanceStatus.RESETTING)

        self.reset_count += 1

        # Stopping the heart
        self.heart_event.clear()

        # Destroying the instance
        self.destroy()
        self.wait_process("destroy", inst_wait=True)

        # Recreating the instance
        self.create()
        self.wait_process("create", inst_wait=True)

        # Instance creation needs additional time until it gets completely configured
        time.sleep(20)

        # Restarting heart
        self.heart_event.set()

    def run_command(self, job_name, command, inst_wait=False, proc_wait=False):

        # Waiting for the instance to be available
        if not inst_wait:
            self.available_event.wait()

        super(PreemptibleInstance, self).run_command(job_name, command, proc_wait=proc_wait)

    def heartbeat(self):

        try:
            # Checking is the heart is active
            if not self.heart_event.is_set():
                return

            # Checking if the instance is alive
            if not self.is_alive():
                self.times_silent += 1
            else:
                self.times_silent = 0

            # Checking if two silent events occured, so that reset is required
            if self.times_silent >= 2:

                # Block processes on main thread
                self.available_event.clear()

                self.reset()
                time.sleep(30)

                # Remounting the mother server
                self.mount(inst_wait=True, proc_wait=True)

                # Restarting the blocked tasks
                for proc_name, proc_obj in self.processes.iteritems():

                    if proc_name in ["create", "destroy", "mount"]:
                        continue

                    cmd = proc_obj.command
                    self.processes[proc_name] = GoogleProcess(cmd, shell=True)

                # Allow processes on main thread
                self.available_event.set()

        except GoogleException:
            print("There was an error on the instance! Restarting the instance soon..")

        finally:
            # Restarting the heartbeat
            self.heart_thread = threading.Timer(self.HEARTBEAT_INTERVAL, self.heartbeat)
            self.heart_thread.daemon = True
            self.heart_thread.start()

    def wait_process(self, proc_name, inst_wait=False):

        # Wait for instance to be available in case is resetting
        if inst_wait == False:
            self.available_event.wait()

        proc_obj = self.processes[proc_name]

        # Skipping processes already done
        if proc_obj.complete:
            return True

        # Ignore other processes than create and destroy if the instance is in reset mode
        if self.status.get() <= InstanceStatus.RESETTING and proc_name not in ["create", "destroy"]:
            return True

        # Waiting for process to finish
        ret_code = proc_obj.wait()

        # Checking if process failed
        if ret_code != 0:

            # Delete process fails when there is nothing to destroy, so move on...
            if proc_name == "destroy":
                return True

            # Create process fails when early preempted or error, so redo it again
            if proc_name == "create":
                proc_obj.complete = True
                raise GoogleException()

            # Wait 20 seconds to see if heartbeat identifies a failure
            time.sleep(20)

            # The process failed because of resetting the instance, so we have to wait for it to be available
            if self.status.get() <= InstanceStatus.RESETTING or not self.available_event.is_set():
                return False

            # The process has actually failed
            print("[%s]--------Process '%s' on instance '%s' failed!" % (datetime.now(), proc_name, self.name))
            self.status.set(InstanceStatus.FAILED)

            proc_obj.complete = True

            # If the instance is preemptible, destroy the instance and the heartbeat will reset it.
            if self.is_preemptible:

                if self.reset_count >= 5:
                    print("[%s]--------Instance %s has already been resetted %d times. The application will be terminated." % (datetime.now(), self.name, self.reset_count))

                print("[%s]--------Instance %s is preemptible, so it will be reset!" % (datetime.now(), self.name))
                self.destroy()

                self.start_heart()
                self.available_event.clear()
                return False
            else:
                raise GoogleException(self.name)

        # Logging the process
        print("[%s]--------Process '%s' on instance '%s' complete!" % (datetime.now(), proc_name, self.name))

        # Changing status if needed
        if proc_name == "create":
            self.status.set(InstanceStatus.IDLE)
        elif proc_name == "destroy":
            self.status.set(InstanceStatus.NONE)

        # Mark as complete
        proc_obj.complete = True

    def wait_all(self, inst_wait=False):
        """ inst_wait is for reset to not wait for the available Event"""

        while True:

            for proc_name, _ in self.processes.iteritems():
                complete = self.wait_process(proc_name, inst_wait=inst_wait)

                # If complete is False, then the process was stopped by unknown events
                if not complete:
                    break

            else:
                # If no breaks in the loop, then all processes are done
                break

        # All processes are finished
        self.status.set(InstanceStatus.IDLE)

    def mount(self, inst_wait=False, proc_wait=False):

        cmd = "sudo mkdir -m a=rwx -p /data && "
        #TODO: remove the next line, when you can make sure that nfs-common is installed from scripts before trying to mount
        cmd += "sudo apt-get install --yes nfs-common && " 
        cmd += "sudo mount -t nfs %s:/data /data" % self.instances["main-server"].name

        self.run_command("mount", cmd, inst_wait=inst_wait, proc_wait=proc_wait)


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

        print("---------Creating %s" % self.name)

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

        print("---------Destroy %s" % self.name)

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
                if proc_name == "create":
                    print("--------Creation %s complete" % self.name)
                elif proc_name == "destroy":
                    print("--------Destroy %s complete" % self.name)
                else:
                    print("--------Process '%s' on instance '%s' complete!" % (proc_name, self.name))
                proc_obj.logged = True


class GoogleCompute(Main):

    def __init__(self, config):
        Main.__init__(self, config)

        self.config         = config

        self.key_location   = "keys/Davelab_GAP_key.json"
        self.authenticate()

        self.instances      = {}
        self.disks          = {}

        self.zone           = self.get_zone()

    def __del__(self):

        # Destroying all the instances
        for instance_name, instance_obj in self.instances.iteritems():
            try:
                instance_obj.destroy()
            except GoogleException:
                print("----Could not set the instance %s for destroy!" % instance_name)

        # Waiting for the instances to be destroyed
        for instance_name, instance_obj in self.instances.iteritems():
            try:
                if instance_name.startswith("split"):
                    instance_obj.wait_all(inst_wait=True)
                else:
                    instance_obj.wait_all()
            except GoogleException:
                print("----Could not destroy instance %s!" % instance_name)

        # Destroying all the disks
        for disk_name, disk_obj in self.disks.iteritems():
            if disk_obj.processes.get("destroy") is None:
                try:
                    disk_obj.destroy()
                except GoogleException:
                    print("---Could not sey the disk %s for destroy!" % disk_name)

        # Waiting for the disks to be destroyed
        for disk_name, disk_obj in self.disks.iteritems():
            try:
                disk_obj.wait_all()
            except GoogleException:
                print("---Could not destroy disk %s!" % disk_name)

    def authenticate(self):

        self.message("Authenticating to the Google Cloud.")

        if not os.path.exists(self.key_location):
            self.error("Authentication key was not found!")

        cmd = "gcloud auth activate-service-account --key-file %s" % self.key_location
        proc = sp.Popen(cmd, shell = True)

        if proc.wait() != 0:
            self.error("Authentication to Google Cloud failed!")

        self.message("Authentication to Google Cloud was successful.")

    def get_zone(self):

        p = sp.Popen(["gcloud config list 2>/dev/null | grep \"zone\""], stdout = sp.PIPE, stderr = sp.PIPE, shell = True)
        output = p.communicate()[0]

        if len(output) != 0:
            return output.strip().split("=")[-1]
        else:
            return "us-east1-b"

    @staticmethod
    def get_instance_type(nr_cpus, mem):

        # Treating special cases
        if nr_cpus == 1:
            if mem <= 0.6:
                return (1, 0.6, "f1-micro")
            if mem <= 1.7:
                return (1, 1.7, "g1-small")
            if mem <= 3.5:
                return (1, 3.5, "n1-standard-1")

        # Defining instance type to mem/cpu ratios
        ratio = {}
        ratio["highcpu"] = 1.80  / 2
        ratio["standard"] = 7.50  / 2
        ratio["highmem"] = 13.00 / 2

        # Identifying needed instance type
        ratio_mem_cpu = mem * 1.0 / nr_cpus
        if ratio_mem_cpu <= ratio["highcpu"]:
            instance_type   = "highcpu"
        elif ratio_mem_cpu <= ratio["standard"]:
            instance_type   = "standard"
        else:
            instance_type   = "highmem"

        # Converting the number of cpus to the closest upper power of 2
        nr_cpus = 2**math.ceil(math.log(nr_cpus, 2))

        # Computing the memory obtain on the instance
        mem = nr_cpus * ratio[instance_type]

        # Setting the instance type name
        inst_type_name = "n1-%s-%d" % (instance_type, nr_cpus)

        return (nr_cpus, mem, inst_type_name)

    def prepare_data(self, sample_data, nr_cpus=None, mem=None, nr_local_ssd=3):

        # Setting the arguments with default values
        if nr_cpus is None:
            nr_cpus = self.config.general.nr_cpus
        if mem is None:
            mem = self.config.general.mem

        # Create the main server
        self.instances["main-server"] = RegularInstance("main-server", nr_cpus, mem,
                                                     is_server=True, nr_local_ssd=nr_local_ssd,
                                                     instances=self.instances)
        self.instances["main-server"].create()
        self.instances["main-server"].wait_process("create")

        # Waiting for the instance to run all the start-up scripts
        time.sleep(120)

        # Getting raw data paths
        R1_path = sample_data["R1_path"]
        R2_path = sample_data["R2_path"]

        # Adding new paths
        sample_data["R1_new_path"] = "/data/%s" % R1_path.split("/")[-1].rstrip(".gz")
        sample_data["R2_new_path"] = "/data/%s" % R2_path.split("/")[-1].rstrip(".gz")

        # Copying input data
        cmd = "gsutil cp %s /data/ " % R1_path
        if R1_path.endswith(".gz"):
            cmd += "; pigz -p %d -d %s" % (max(nr_cpus/2, 1), sample_data["R1_new_path"])
        self.instances["main-server"].run_command("copyFASTQ_R1", cmd)

        cmd = "gsutil cp %s /data/ " % R2_path
        if R2_path.endswith(".gz"):
            cmd += "; pigz -p %d -d %s" % (max(nr_cpus/2, 1), sample_data["R2_new_path"])
        self.instances["main-server"].run_command("copyFASTQ_R2", cmd)

        # Copying and configuring the softwares
        cmd = "gsutil -m cp -r gs://davelab_data/src /data/ ; bash /data/src/setup.sh"
        self.instances["main-server"].run_command("copySrc", cmd)

        # Waiting for all the copying processes to be done
        self.instances["main-server"].wait_all()

    def create_split_servers(self, nr_splits, nr_cpus=None, mem=None, nr_local_ssd=1, is_preemptible=True):
        # Memorize the number of splits for later use
        if nr_cpus is None:
            nr_cpus = self.config.general.nr_cpus
        if mem is None:
            mem = self.config.general.mem

        # Creating the split servers
        for split_id in xrange(nr_splits):
            name = "split%d-server" % split_id
            if is_preemptible:
                self.instances[name] = PreemptibleInstance(name, nr_cpus, mem, is_server=False,
                                                           nr_local_ssd=nr_local_ssd, instances=self.instances)
            else:
                self.instances[name] = RegularInstance(name, nr_cpus, mem, is_server=False,
                                                           nr_local_ssd=nr_local_ssd, instances=self.instances)
            self.instances[name].create()

        # Waiting for the servers to be created
        failed_create = list()
        creation_trial = 0
        while True:

            # Counting the number of retrials
            if creation_trial == 3:
                raise GoogleException()
            creation_trial += 1

            # Recreating previously failed servers
            for failed_instance in failed_create:
                self.instances[failed_instance].destroy()
                self.instances[failed_instance].wait_process("destroy", inst_wait=True)
                self.instances[failed_instance].create()

            failed_create = list()

            # Waiting for the instances to get created
            for instance_name, instance_obj in self.instances.iteritems():
                if instance_name.startswith("split"):
                    try:
                        instance_obj.wait_process("create")
                        instance_obj.start_heart()

                    except GoogleException:
                        # Failed to create, retry
                        failed_create.append(instance_name)

            # Stopping the loop if no instances failed
            if not failed_create:
                break

        # Waiting for instances to run the start-up scripts
        time.sleep(120)

        # Mounting the mother to split server
        for instance_name, instance_obj in self.instances.iteritems():
            if instance_name.startswith("split"):
                instance_obj.mount()

        # Waiting for the mounting
        for instance_name, instance_obj in self.instances.iteritems():
            if instance_name.startswith("split"):
                instance_obj.wait_process("mount")

    def finalize(self, sample_data):

        # Exiting if no outputs are present
        if "outputs" not in sample_data:
            return None

        # Copying the output data
        for i, output_path in enumerate(sample_data["outputs"]):
            cmd = "gsutil -m cp -r %s gs://davelab_temp/" % output_path
            self.instances["main-server"].run_command("copyOut_%d" % i, cmd)

        # Waiting for all the copying processes to be done
        self.instances["main-server"].wait_all()

    def validate(self):
        pass
