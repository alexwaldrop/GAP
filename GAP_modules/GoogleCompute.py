import math
import os
import subprocess as sp
import threading
import time

from GAP_interfaces import Main

class GoogleResource():

    def __init__(self, name, type_, create_process=None):

        self.name   = name
        self.type_  = type_

        self.set_create_process(create_process)
        self.destroy_process = None

    def set_create_process(self, proc):

        if proc is None:
            self.create_process = None
        elif isinstance(proc, GoogleProcess):
            self.create_process = proc
        else:
            self.create_process = GoogleProcess("create_%s" % (self.name), proc)

    def get_create_process(self):
        return self.create_process

    def set_destroy_process(self, proc):

        if proc is None:
            self.destroy_process = None
        elif isinstance(proc, GoogleProcess):
            self.destroy_process = proc
        else:
            self.destroy_process = GoogleProcess("destroy_%s" % (proc), proc)

    def get_destroy_process(self):
        return self.destroy_process

    def get_type(self):
        return self.type_

    def is_ready(self):
        return self.create_process is not None and self.create_process.is_done()

    def is_dead(self):
        return self.destroy_process is not None and self.destroy_process.is_done()


class GoogleProcess():

    def __init__(self, name, process):

        self.name = name

        self.proc = process
        self.return_code = None

    def is_done(self):
        return self.get_return_code() is not None

    def get_return_code(self, wait=False):

        if self.return_code is None:
            if wait:
                self.return_code = self.proc.wait()
            else:
                self.return_code = self.proc.poll()

        return self.return_code


class GoogleCompute(Main):

    def __init__(self, config):
        Main.__init__(self, config)

        self.key_location   = "keys/Davelab_GAP_key.json"
        self.authenticate()
        
        self.prefix         = "gap-"

        self.resources      = {}
        self.main_server    = None

        self.zone           = self.getZone()

    def __del__(self):

        while self.resources:

            to_remove = []

            for resource_name, resource in self.resources.iteritems():

                if resource.get_destroy_process() is None:
                    if resource.get_type() == "instance":
                        proc = self.destroyInstance(resource_name)
                    elif resource.get_type() == "disk":
                        proc = self.destroyDisk(resource_name)
                    else:
                        proc = None

                    resource.set_destroy_process(proc)

                elif resource.is_dead():
                    to_remove.append(resource_name)

            for resource_name in to_remove:
                del self.resources[resource_name]

            time.sleep(5)

    def authenticate(self):

        self.message("Authenticating to the Google Cloud.")

        if not os.path.exists(self.key_location):
            self.error("Authentication key was not found!")

        proc = GoogleProcess("authenticate", sp.Popen(["gcloud auth activate-service-account --key-file %s" % self.key_location], shell = True))

        if proc.get_return_code(wait=True) != 0:
            self.error("Authentication to Google Cloud failed!")

        self.message("Authentication to Google Cloud was successful.")

    def getInstanceType(self, cpus, mem):
            
        # Treating special cases
        if cpus == 1:
            if mem <= 0.6:
                return "f1-micro"
            if mem <= 1.7:
                return "g1-small"
            if mem <= 3.5:
                return "n1-standard-1"

        # Defining instance type to mem/cpu ratios
        ratio_high_cpu  = 1.80  / 2
        ratio_standard  = 7.50  / 2
        ratio_high_mem  = 13.00 / 2

        # Identifying needed instance type
        ratio_mem_cpu = mem * 1.0 / cpus
        if ratio_mem_cpu <= ratio_high_cpu:
            instance_type   = "highcpu"
        elif ratio_mem_cpu <= ratio_standard:
            instance_type   = "standard"
        else:
            instance_type   = "highmem"

        # Converting the number of cpus to the closest upper power of 2
        nr_cpus = 2**math.ceil(math.log(cpus, 2))
    
        # Returning instance name
        return "n1-%s-%d" % (instance_type, nr_cpus)

    def getZone(self):
        
        p = sp.Popen(["gcloud config list 2>/dev/null | grep \"zone\""], stdout = sp.PIPE, stderr = sp.PIPE, shell = True)
        output = p.communicate()[0]

        if len(output) != 0:
            return output.strip().split("=")[-1]
        else:
            return "us-east1-b"

    def prepareData(self, sample_data, nr_cpus = 32, nr_local_ssd = 3, split=False, nr_splits=23, preemptible_splits=True):
        # Obtaining the needed type of instance
        instance_type   = self.getInstanceType(nr_cpus, 2 * nr_cpus)

        # Create the main server
        proc = self.createFileServer("main-server", instance_type, nr_local_ssd=nr_local_ssd)
        resource = GoogleResource("main-server", "instance", create_process=proc)
        self.resources["main-server"] = resource

        self.main_server = "main-server"

        # Memorize the number of splits for later use
        self.nr_splits = nr_splits

        # Creating the split servers
        if split:
            for i in xrange(nr_splits):
                proc = self.createFileServer("split%d-server" % i, instance_type, nr_local_ssd=1, is_preemptible=preemptible_splits)
                resource = GoogleResource("split%d-server" % i, "instance", create_process=proc)
                self.resources["split%d-server" % i] = resource

        # Waiting for the servers to be created
        while not all( resource.is_ready() for _, resource in self.resources.iteritems() ):
            time.sleep(5)

        # Waiting for the instance to run all the start-up scripts
        time.sleep(120)

        # Getting raw data paths
        R1_path = sample_data["R1_path"]
        R2_path = sample_data["R2_path"]

        # Adding new paths
        sample_data["R1_new_path"] = "/data/%s" % R1_path.split("/")[-1].rstrip(".gz")
        sample_data["R2_new_path"] = "/data/%s" % R2_path.split("/")[-1].rstrip(".gz")

        # Identifying if decompression is needed
        with_decompress = { "R1": R1_path.endswith(".gz"),
                            "R2": R2_path.endswith(".gz") }

        # Creating list of processes
        wait_list = []

        # Copying input data
        cmd = "gsutil cp %s /data/ " % R1_path
        if with_decompress["R1"]:
            cmd += "; pigz -p %d -d %s" % (max(nr_cpus/2, 1), sample_data["R1_new_path"])
        wait_list.append(
            GoogleProcess("copyFASTQ_R1",
                self.runCommand("copyFASTQ_R1", cmd, on_instance=self.main_server)
            )
        )

        cmd = "gsutil cp %s /data/ " % R2_path
        if with_decompress["R2"]:
            cmd += "; pigz -p %d -d %s" % (max(nr_cpus/2, 1), sample_data["R2_new_path"])
        wait_list.append(
            GoogleProcess("copyFASTQ_R2",
                self.runCommand("copyFASTQ_R2", cmd, on_instance=self.main_server)
            )
        )

        # Copying the reference genome
        cmd = "mkdir -p /data/ref/; gsutil -m cp -r gs://davelab_data/ref/hg19/* /data/ref/"
        wait_list.append(
            GoogleProcess("copyRef",
                self.runCommand("copyRef", cmd, on_instance=self.main_server)
            )
        )
        if split:
            for i in xrange(nr_splits):
                wait_list.append(
                    GoogleProcess("copyRef_split%d" % i,
                        self.runCommand("copyRef_split%d" % i, cmd, on_instance="split%d-server" % i)
                    )
                )

        # Copying and configuring the softwares
        cmd = "gsutil -m cp -r gs://davelab_data/src /data/ && bash /data/src/setup.sh"
        wait_list.append(
            GoogleProcess("copySrc",
                self.runCommand("copySrc", cmd, on_instance=self.main_server)
            )
        )
        if split:
            for i in xrange(nr_splits):
                wait_list.append(
                    GoogleProcess("copySrc_split%d" % i,
                        self.runCommand("copySrc_split%d" % i, cmd, on_instance="split%d-server" % i)
                    )
                )

        # Creating split directory and mount split server
        if split:
            for i in xrange(nr_splits):
                cmd = "mkdir -p /data/split%d && " % i
                cmd += "sudo mount -t nfs split%d-server:/data /data/split%d" % (i, i)

                wait_list.append(
                    GoogleProcess("mountSplit%d" % i,
                        self.runCommand("mountSplit%d" % i, cmd, on_instance=self.main_server)
                    )
                )

        # Waiting for all the copying processes to be done
        while not all( proc.is_done() for proc in wait_list ):
            time.sleep(5)

    def finalize(self, sample_data):

        # Exiting if no outputs are present
        if "outputs" not in sample_data:
            return None

        # Creating list of processes
        wait_list = []

        # Copying the output data
        for i, output_path in enumerate(sample_data["outputs"]):
            cmd = "gsutil -m cp -r %s gs://davelab_temp/" % output_path
            wait_list.append(
                GoogleProcess("copyOut_%d" % i,
                    self.runCommand("copyOut_%d" % i, cmd, on_instance=self.main_server)
                )
            )

        # Waiting for all the copying processes to be done
        while not all( proc.is_done() for proc in wait_list ):
            time.sleep(5)

    def createDisk(self, name, size, is_SSD = False, zone = None, with_image = False):
        
        self.message("Creating persistent disk '%s'." % name)

        args = ["gcloud compute disks create %s" % name]

        args.append("--size")
        if size >= 1024:
            args.append("%dTB" % int(size/1024))
        else:
            args.append("%dGB" % int(size))

        args.append("--type")
        if is_SSD:
            args.append("pd-ssd")
        else:
            args.append("pd-standard")

        args.append("--zone")
        if zone is None:
            args.append(self.zone)
        else:
            args.append(zone)

        if with_image:
            args.append("--image")
            args.append("ubuntu-14-04")

        with open(os.devnull, "w") as devnull:
            return sp.Popen(" ".join(args), stdout=devnull, stderr=devnull, shell=True)

    def createInstance(self, name, instance_type, boot_disk_size = 10, is_boot_disk_ssd = False, is_preemptible = False, zone = None, nr_local_ssd = 0, start_up_script = None):

        self.message("Creating instance '%s'." % name)

        args = ["gcloud compute instances create %s" % name]

        args.append("--boot-disk-size")
        if boot_disk_size >= 1024:
            args.append("%dTB" % int(boot_disk_size/1024))
        else:
            args.append("%dGB" % int(boot_disk_size))

        args.append("--boot-disk-type")
        if is_boot_disk_ssd:
            args.append("pd-ssd")
        else:
            args.append("pd-standard")

        args.extend(["--local-ssd" for _ in xrange(nr_local_ssd)])

        args.append("--scopes")
        args.append("gap-412@davelab-gcloud.iam.gserviceaccount.com=\"https://www.googleapis.com/auth/cloud-platform\"")

        args.append("--image")
        args.append("ubuntu-14-04")

        args.append("--machine-type")
        args.append(instance_type)

        if is_preemptible:
            args.append("--preemptible")

        if start_up_script is not None:
            args.append("--metadata")
            args.append("startup-script-url=gs://davelab_data/scripts/%s" % start_up_script)
        elif nr_local_ssd != 0:
            args.append("--metadata")
            args.append("startup-script-url=gs://davelab_data/scripts/LocalSSD.sh")

        args.append("--zone")
        if zone is None:
            args.append(self.zone)
        else:
            args.append(zone)

        with open(os.devnull, "w") as devnull:
            return sp.Popen(" ".join(args), stdout=devnull, stderr=devnull, shell=True)

    def createFileServer(self, name, instance_type, boot_disk_size = 10, is_boot_disk_ssd = False, is_preemptible = False, zone = None, nr_local_ssd = 0):

        if nr_local_ssd == 0:
            start_up_script = "nfs.sh"
        else:
            start_up_script = "nfs_LocalSSD.sh"

        return self.createInstance(name, instance_type,
                            boot_disk_size      = boot_disk_size,
                            is_boot_disk_ssd    = is_boot_disk_ssd,
                            is_preemptible      = is_preemptible,
                            zone                = zone,
                            nr_local_ssd        = nr_local_ssd,
                            start_up_script     = start_up_script)

    def attachDisk(self, disk_name, instance_name, zone = None, is_read_only = True):

        self.message("Attaching disk '%s' to instance '%s'." % (disk_name, instance_name))

        args = ["gcloud compute instances attach-disk %s" % instance_name]

        args.append("--disk")
        args.append(disk_name)

        args.append("--mode")
        if is_read_only:
            args.append("ro")
        else:
            args.append("rw")

        args.append("--zone")
        if zone is None:
            args.append(self.zone)
        else:
            args.append(zone)

        with open(os.devnull, "w") as devnull:
            return sp.Popen(" ".join(args), stdout=devnull, stderr=devnull, shell=True)

    def detachDisk(self, disk_name, instance_name, zone = None):

        self.message("Detaching disk '%s' from instance '%s'." % (disk_name, instance_name))

        args = ["gcloud compute instances detach-disk %s" % instance_name]

        args.append("--disk")
        args.append(disk_name)

        args.append("--zone")
        if zone is None:
            args.append(self.zone)
        else:
            args.append(zone)

        with open(os.devnull, "w") as devnull:
            return sp.Popen(" ".join(args), stdout=devnull, stderr=devnull, shell=True)

    def destroyDisk(self, name, zone = None):

        self.message("Destroying disk '%s'." % name)

        args = ["gcloud compute disks delete %s" % name]

        args.append("--zone")
        if zone is None:
            args.append(self.zone)
        else:
            args.append(zone)
        
        # Provide input to the command
        args[0:0] = ["yes", "2>/dev/null", "|"]

        with open(os.devnull, "w") as devnull:
            return sp.Popen(" ".join(args), stdout=devnull, stderr=devnull, shell=True)

    def destroyInstance(self, name, zone = None):

        self.message("Destroying instance '%s'." % name)

        args = ["gcloud compute instances delete %s" % name]

        args.append("--zone")
        if zone is None:
            args.append(self.zone)
        else:
            args.append(zone)

        # Provide input to the command
        args[0:0] = ["yes", "2>/dev/null", "|"]

        with open(os.devnull, "w") as devnull:
            return sp.Popen(" ".join(args), stdout=devnull, stderr=devnull, shell=True)

    def runCommand(self, job_name, command, on_instance = None, cpus = 1, mem = 1):

        if on_instance is None:
            inst_type = self.getInstanceType(cpus, mem)
            inst_name = self.prefix + job_name

            proc = self.createInstance(inst_name, inst_type)
            self.resources[inst_name] = GoogleResource(inst_name, "instance", create_process=proc)

            # Waiting for instance to get ready
            self.resources[inst_name].get_return_code(wait=True)
            time.sleep(10)
        else:
            inst_name = on_instance

        cmd = "gcloud compute ssh gap@%s --command '%s'" % (inst_name, command)

        return sp.Popen(cmd, shell=True)

    def validate(self):
        pass
