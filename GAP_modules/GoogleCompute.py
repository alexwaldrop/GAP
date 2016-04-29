import math
import os
import subprocess as sp
import threading
import time

from GAP_interfaces import Main

class Instance():

    def __init__(self, name, typ, cmdP):
        self.name   = name
        self.type   = typ

        self.cmdP       = cmdP
        self.destroyP   = None

    def isBusy(self):

        if self.cmdP.poll() is None:
            return True
        elif self.destroyP is not None and self.destroyP.poll() is None:
            return True

        return False

    def isDead(self):

        return self.destroyP is not None and self.destroyP.poll() is not None

class GoogleCompute(Main):

    def __init__(self, config):
        Main.__init__(self, config)

        self.key_location   = "keys/Davelab_GAP_key.json"
        self.authenticate()
        
        self.prefix         = "gap-"

        self.instances      = []
        self.main_server    = None
        sweeper             = threading.Timer(60.0, self.cleanPlatform)
        sweeper.start()

        self.zone           = self.getZone()

    def __del__(self):

        self.cleanPlatform(force=True)

        if self.main_server is not None:
            self.destroyInstance(self.main_server).wait()

        while len(self.instances):
            self.instances = [ inst for inst in self.instances if not inst.isDead() ]

            time.sleep(5)

    def authenticate(self):

        self.message("Authenticating to the Google Cloud.")

        if not os.path.exists(self.key_location):
            self.error("Authentication key was not found!")

        return_code = sp.Popen(["gcloud auth activate-service-account --key-file %s" % self.key_location],
                                    shell = True).wait()

        if return_code != 0:
            self.error("Authentication to Google Cloud failed!")

        self.message("Authentication to Google Cloud was successful.")

    def cleanPlatform(self, force = False):

        self.instances = [ inst for inst in self.instances if not inst.isDead() ]

        for inst in self.instances:
            if inst.destroyP is None and (not inst.isBusy() or force):
                inst.destroyP = self.destroyInstance(inst.name)

    def getInstanceType(self, cpus, mem):
            
        # Treating special cases
        if cpus == 1:
            if mem <= 0.6:
                return "f1-micro"
            if mem <= 1.7:
                return "g1-small"
            if mem <= 3.5:
                return "n1-standard-1"

        # Defining instance type to cpu/mem ratios
        ratio_high_cpu  = 2/1.80
        ratio_standard  = 2/7.50
        ratio_high_mem  = 2/13.00

        # Identifying needed instance type
        ratio_cpu_mem = cpus*1.0/mem
        if ratio_cpu_mem <= ratio_high_mem:
            instance_type   = "highmem"
        elif ratio_cpu_mem <= ratio_standard:
            instance_type   = "standard"
        else:
            instance_type   = "highcpu"            

        # Converting the number of cpus to the closest power of 2
        nr_cpus = 2**int(math.log(cpus, 2))
    
        # Returning instance name
        return "n1-%s-%d" % (instance_type, nr_cpus)

    def getZone(self):
        
        p = sp.Popen(["gcloud config list 2>/dev/null | grep \"zone\""], stdout = sp.PIPE, stderr = sp.PIPE, shell = True)
        output = p.communicate()[0]

        if len(output) != 0:
            return output.strip().split("=")[-1]
        else:
            return "us-east1-b"

    def prepareData(self, sample_data, nr_cpus = 32, nr_local_ssd = 3):

        # Obtaining the needed type of instance
        instance_type   = self.getInstanceType(nr_cpus, 2 * nr_cpus)

        # Create the main server
        self.createFileServer("main" , instance_type, nr_local_ssd = nr_local_ssd)

        # Waiting for the instance to run all the start-up scripts
        time.sleep(100)

        # Getting raw data paths
        R1_path = sample_data["R1_path"]
        R2_path = sample_data["R2_path"]

        # Creating list of processes
        wait_list = []

        # Copying input data
        wait_list.append(self.runCommand("copyFASTQ_R1", "gsutil cp %s /data/" % R1_path, on_instance=self.main_server))
        wait_list.append(self.runCommand("copyFASTQ_R2", "gsutil cp %s /data/" % R2_path, on_instance=self.main_server))

        # Copying the reference genome
        wait_list.append(self.runCommand("copyRef", "mkdir -p /data/ref/; gsutil -m cp -r gs://davelab_data/ref/hg19/* /data/ref/", on_instance=self.main_server))

        # Copying and configuring the softwares
        wait_list.append(self.runCommand("copySrc", "gsutil -m cp -r gs://davelab_data/src /data/ && bash /data/src/setup.sh", on_instance=self.main_server))

        # Waiting for all the copying processes to be done
        done = False
        while not done:
            done = True
            for proc in wait_list:
                if proc.poll() == None:
                    done = False

        # Adding new paths
        sample_data["R1_new_path"] = "/data/%s" % sample_data["R1_path"].split("/")[-1]
        sample_data["R2_new_path"] = "/data/%s" % sample_data["R2_path"].split("/")[-1]


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

    def createFileServer(self, name, instance_type, boot_disk_size = 10, is_boot_disk_ssd = False, zone = None, nr_local_ssd = 0):

        server_name = name + "-server"
        self.message("Creating File Server '%s'." % server_name)

        if nr_local_ssd == 0:
            start_up_script = "nfs.sh"
        else:
            start_up_script = "nfs_LocalSSD.sh"

        proc = self.createInstance(server_name, instance_type,
                            boot_disk_size      = boot_disk_size,
                            is_boot_disk_ssd    = is_boot_disk_ssd,
                            zone                = zone,
                            nr_local_ssd        = nr_local_ssd,
                            start_up_script     = start_up_script)
        return_code = proc.wait()

        self.main_server = server_name

        # Waiting for the instance to get ready
        if return_code != 0:
            self.error("File Server '%s' could not be created!" % server_name)
        else:
            time.sleep(15)
            self.message("File Server '%s' is up and running." % server_name)

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
            inst_type   = self.getInstanceType(cpus, mem)
            inst_name   = self.prefix + job_name
            self.createInstance(inst_name, inst_type).wait()

            # Waiting for instance to get ready
            time.sleep(10)
        else:
            inst_name   = on_instance

        cmd = "gcloud compute ssh gap@%s --command '%s'" % (inst_name, command)

        proc = sp.Popen(cmd, shell=True)
        if on_instance is None:
            self.instances.append( Instance(inst_name, inst_type, proc) )

        return proc

    def validate(self):
        pass
