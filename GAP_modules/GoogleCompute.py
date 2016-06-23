import math
import os
import subprocess as sp
import threading
import time

from GAP_interfaces import Main

class Instance():

    def __init__(self, name, nr_cpus, mem, **kwargs):

        # Setting variables
        self.name           = name
        self.nr_cpus, self.mem, self.instance_type = GoogleCompute.get_instance_type(nr_cpus, mem)

        self.is_server          = kwargs.get("is_server",        False)
        self.boot_disk_size     = kwargs.get("boot_disk_size",   15)
        self.is_boot_disk_ssd   = kwargs.get("is_boot_disk_ssd", False)
        self.is_preemptible     = kwargs.get("is_preemptible",   False)
        self.zone               = kwargs.get("zone",             "us-east1-b")
        self.nr_local_ssd       = kwargs.get("nr_local_ssd",     0)
        self.start_up_script    = kwargs.get("start_up_script",  None)

        self.attached_disks     = []
        self.processes          = {}

    def create(self):

        print("--------Creating %s" % self.name)

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

        args.extend(["--local-ssd" for _ in xrange(self.nr_local_ssd)])

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
            args.append("startup-script-url=gs://davelab_data/scripts/%s" % start_up_script)
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
            if self.nr_local_ssd == 1:
                args.append("--metadata")
                args.append("startup-script-url=gs://davelab_data/scripts/LocalSSD.sh")
            elif self.nr_local_ssd > 1:
                args.append("--metadata")
                args.append("startup-script-url=gs://davelab_data/scripts/LocalSSD_RAID.sh")

        args.append("--zone")
        args.append(self.zone)

        with open(os.devnull, "w") as devnull:
            self.processes["create"] = sp.Popen(" ".join(args), stdout=devnull, stderr=devnull, shell=True)

    def destroy(self):

        print("--------Destroying %s" % self.name)

        args = ["gcloud compute instances delete %s" % self.name]

        args.append("--zone")
        args.append(self.zone)

        # Provide input to the command
        args[0:0] = ["yes", "2>/dev/null", "|"]

        with open(os.devnull, "w") as devnull:
            self.processes["destroy"] = sp.Popen(" ".join(args), stdout=devnull, stderr=devnull, shell=True)

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
            self.processes["attach_disk_%s" % disk.name] = sp.Popen(" ".join(args), stdout=devnull, stderr=devnull, shell=True)

    def detach_disk(self, disk):

        args = ["gcloud compute instances detach-disk %s" % self.name]

        args.append("--disk")
        args.append(disk.name)

        args.append("--zone")
        args.append(self.zone)

        self.attached_disks.remove(disk)

        with open(os.devnull, "w") as devnull:
            self.processes["detach_disk_%s" % disk.name] =  sp.Popen(" ".join(args), stdout=devnull, stderr=devnull, shell=True)

    def run_command(self, job_name, command, wait=False):

        cmd = "gcloud compute ssh gap@%s --command '%s'" % (self.name, command)

        self.processes[job_name] = sp.Popen(cmd, shell=True)

        if wait:
            self.processes[job_name].wait()

    def wait_all(self):

        for proc_name, proc_obj in self.processes.iteritems():
            proc_obj.wait()

            # Logging if not logged yet
            if not hasattr(proc_obj, 'logged'):
                if proc_name == "create":
                    print("--------Creation %s complete" % self.name)
                elif proc_name == "destroy":
                    print("--------Destroy %s complete" % self.name)
                else:
                    print("--------Process '%s' on instance '%s' complete!" % (proc_name, self.name))
                proc_obj.logged = True


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
            self.processes["create"] = sp.Popen(" ".join(args), stdout=devnull, stderr=devnull, shell=True)

    def destroy(self):

        print("---------Destroy %s" % self.name)

        args = ["gcloud compute disks delete %s" % self.name]

        args.append("--zone")
        args.append(self.zone)

        # Provide input to the command
        args[0:0] = ["yes", "2>/dev/null", "|"]

        with open(os.devnull, "w") as devnull:
            self.processes["destroy"] = sp.Popen(" ".join(args), stdout=devnull, stderr=devnull, shell=True)

    def wait_all(self):

        for proc_name, proc_obj in self.processes.iteritems():
            proc_obj.wait()

            # Logging if not logged yet
            if not hasattr(proc_obj, 'logged'):
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
        for _, instance_obj in self.instances.iteritems():
            if instance_obj.processes.get("destroy") is None:
                instance_obj.destroy()

        # Waiting for the instances to be destroyed
        for _, instance_obj in self.instances.iteritems():
            instance_obj.processes["destroy"].wait()

        # Destroying all the disks
        for _, disk_obj in self.disks.iteritems():
            if disk_obj.processes.get("destroy") is None:
                disk_obj.destroy()

        # Waiting for the disks to be destroyed
        for _, disk_obj in self.disks.iteritems():
            disk_obj.processes["destroy"].wait()

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
        self.instances["main-server"] = Instance("main-server", nr_cpus, mem, is_server=True, nr_local_ssd=nr_local_ssd)
        self.instances["main-server"].create()
        self.instances["main-server"].wait_all()

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
        cmd = "gsutil -m cp -r gs://davelab_data/src /data/ && bash /data/src/setup.sh"
        self.instances["main-server"].run_command("copySrc", cmd)

        # Waiting for all the copying processes to be done
        self.instances["main-server"].wait_all()

    def create_split_servers(self, nr_splits, nr_cpus=None, mem=None, nr_local_ssd=1, is_preemptible=True):
        # Memorize the number of splits for later use
        if nr_cpus is None:
            nr_cpus = self.config.general.nr_cpus

        # Creating the split servers
        for split_id in xrange(nr_splits):
            name = "split%d-server" % split_id
            self.instances[name] = Instance(name, nr_cpus, 2*nr_cpus, is_server=True, nr_local_ssd=nr_local_ssd, is_preemptible=is_preemptible)
            self.instances[name].create()

        # Waiting for the servers to be created
        for instance_name, instance_obj in self.instances.iteritems():
            if instance_name.startswith("split"):
                instance_obj.wait_all()

        # Waiting for instances to run the start-up scripts
        time.sleep(90)

        # Copying and configuring the softwares
        cmd = "gsutil -m cp -r gs://davelab_data/src /data/ && bash /data/src/setup.sh"
        for split_id in xrange(nr_splits):
            self.instances["split%d-server" % split_id].run_command("copySrc", cmd)

        # Waiting for the data to be copied
        for instance_name, instance_obj in self.instances.iteritems():
            if instance_name.startswith("split"):
                instance_obj.wait_all()

        # Creating split directory and mount split server
        for split_id in xrange(nr_splits):
            cmd = "mkdir -p /data/split%d && " % split_id
            cmd += "sudo mount -t nfs split%d-server:/data /data/split%d" % (split_id, split_id)
            self.instances["main-server"].run_command("mountSplit%d" % split_id, cmd)

        self.instances["main-server"].wait_all()

    def finalize(self, sample_data):

        # Exiting if no outputs are present
        if "outputs" not in sample_data:
            return None

        # Creating list of processes
        wait_list = []

        # Copying the output data
        for i, output_path in enumerate(sample_data["outputs"]):
            cmd = "gsutil -m cp -r %s gs://davelab_temp/" % output_path
            self.instances["main-server"].run_command("copyOut_%d" % i, cmd)

        # Waiting for all the copying processes to be done
        self.instances["main-server"].wait_all()

    def validate(self):
        pass
