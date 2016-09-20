import importlib
import time

from GAP_interfaces import Main

class Node(Main):

    def __init__(self, config, platform, sample_data, module_name):

        Main.__init__(self, config)

        self.config = config
        self.platform = platform
        self.sample_data = sample_data

        # Importing main module
        try:
            self.main = self.initialize_module(module_name)
            self.main["instance"] = self.main["class"](self.config)
        except:
            self.error("Module %s cannot be imported!" % module_name)

        # Importing splitter and merger:
        if self.main["instance"].can_split:

            try:
                self.split = self.initialize_module(self.main["instance"].splitter)
                self.split["instance"] = self.split["class"](self.config)
            except:
                self.error("Module %s cannot be imported!" % self.main["instance"].splitter)

            try:
                self.merge = self.initialize_module(self.main["instance"].merger)
                self.merge["instance"] = self.merge["class"](self.config)
            except:
                self.error("Module %s cannot be imported!" % self.main["instance"].merger)

        self.process = None

    def initialize_module(self, module_name):

        d = {}
        d["module_name"]    = module_name
        d["module"]         = importlib.import_module("GAP_modules.%s" % d["module_name"])
        d["class_name"]     = d["module"].__main_class__
        d["class"]          = d["module"].__dict__[ d["class_name"] ] 

        return d

    def run_split(self):

        # Setting up the splitter
        self.split["instance"].R1 = self.sample_data["R1_new_path"]
        self.split["instance"].R2 = self.sample_data["R2_new_path"]
        self.split["instance"].nr_splits = self.config.general.nr_splits

        # Running the splitter
        cmd = self.split["instance"].getCommand()
        self.platform.instances["main-server"].run_command("split", cmd)
        self.platform.instances["main-server"].wait_all()

        # Creating the split servers
        self.platform.create_split_servers(self.config.general.nr_splits, nr_cpus=self.config.general.nr_cpus,
                                           is_preemptible=True, nr_local_ssd=0)

        # TODO: PASS DATA TO MAIN MODULE IN A NICER WAY

        # Calling the process on each split
        for split_id in xrange(self.config.general.nr_splits):
            self.main["instance"].R1 = "%s/fastq_R1_%02d" % (self.config.general.temp_dir, split_id)
            self.main["instance"].R2 = "%s/fastq_R2_%02d" % (self.config.general.temp_dir, split_id)
            self.main["instance"].threads = self.config.general.nr_cpus
            self.main["instance"].split_id = split_id

            cmd = self.main["instance"].getCommand()

            self.platform.instances["split%d-server" % split_id].run_command("align%d" % split_id, cmd)

        # Waiting for all the split aligning processes to finish
        while True:

            still_running = False

            for instance_name, instance_obj in self.platform.instances.iteritems():
                if instance_name.startswith("split"):

                    # Generating process name
                    split_id = int(instance_name.split("-")[0].split("split")[-1])
                    proc_name = "align%d" % split_id

                    # Check if process is done
                    if instance_obj.poll_process(proc_name):

                        # Skipping instances that are resetting
                        if instance_obj.is_preemptible and not instance_obj.available_event.is_set():
                            continue

                        # Skipping process that has already been registered
                        if instance_obj.processes[proc_name].complete:
                            continue

                        # Skipping process that has previously failed and not marked complete
                        if instance_obj.processes[proc_name].has_failed():
                            still_running = True
                            continue

                        # Checking the complete process
                        instance_obj.wait_process(proc_name)

                        # Destroy the split
                        instance_obj.destroy()

                    else:
                        still_running = True

            if still_running:
                time.sleep(5)
            else:
                break

        # Setting up the merger
        self.merge["instance"].nr_splits = self.config.general.nr_splits
        self.merge["instance"].threads = self.config.general.nr_cpus

        # Running the merger
        cmd = self.merge["instance"].getCommand()
        self.platform.instances["main-server"].run_command("merge", cmd)

        # Waiting for all the split servers to DIE!!!
        for instance_name, instance_obj in self.platform.instances.iteritems():
            if instance_name.startswith("split"):
                instance_obj.wait_all()

        # Waiting for the merging to finish
        self.platform.instances["main-server"].wait_all()

        # Marking for output
        self.sample_data["outputs"] = ["%s/%s.bam" % (self.config.general.temp_dir, self.config.general.sample_name)]

    def run_normal(self):

        self.main["instance"].R1 = self.sample_data["R1_new_path"]
        self.main["instance"].R2 = self.sample_data["R2_new_path"]
        self.main["instance"].threads = self.config.general.nr_cpus

        cmd = self.main["instance"].getCommand()

        self.platform.instances["main-server"].run_command("align", cmd)
        self.platform.instances["main-server"].wait_all()

        # Marking for output
        self.sample_data["outputs"] = ["%s/out.bam" % self.config.general.temp_dir]

    def run(self):

        if self.main["instance"].can_split and self.config.general.split:
            self.run_split()
        else:
            self.run_normal()

    def validate(self):
        pass