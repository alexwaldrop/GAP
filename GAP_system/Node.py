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
        self.platform.create_split_servers(self.config.general.nr_splits, nr_cpus=self.config.general.nr_cpus, is_preemptible=False)

        cmds = []

        # Moving the splits in their folders
        for split_id in xrange(self.config.general.nr_splits):
            cmds.append("mv %s/fastq_R1_%02d %s/split%d/" % (self.config.general.temp_dir, split_id, self.config.general.temp_dir, split_id) )
            cmds.append("mv %s/fastq_R2_%02d %s/split%d/" % (self.config.general.temp_dir, split_id, self.config.general.temp_dir, split_id) )

        self.platform.instances["main-server"].run_command("move_splits", " && ".join(cmds))
        self.platform.instances["main-server"].wait_all()

        # Calling the process on each split
        for split_id in xrange(self.config.general.nr_splits):
            self.main["instance"].R1 = "%s/fastq_R1_%02d" % (self.config.general.temp_dir, split_id)
            self.main["instance"].R2 = "%s/fastq_R2_%02d" % (self.config.general.temp_dir, split_id)
            self.main["instance"].threads = self.config.general.nr_cpus

            cmd = self.main["instance"].getCommand()

            self.platform.instances["split%d-server" % split_id].run_command("align%d" % split_id, cmd)

        # Waiting for all the split aligning processes to finish
        for instance_name, instance_obj in self.platform.instances.iteritems():
	    if instance_name.startswith("split"):
	        instance_obj.wait_all()

	    # Setting up the merger
        self.merge["instance"].nr_splits = self.config.general.nr_splits
        self.merge["instance"].threads = self.config.general.nr_cpus

        # Running the merger
        cmd = self.merge["instance"].getCommand()
        self.platform.instances["main-server"].run_command("merge", cmd)
        self.platform.instances["main-server"].wait_all()

        # Destroying the splits
        for instance_name, instance_obj in self.platform.instances.iteritems():
	    if instance_name.startswith("split"):
	        instance_obj.destroy()

        # Waiting for all the split servers to DIE!!!
        for instance_name, instance_obj in self.platform.instances.iteritems():
	    if instance_name.startswith("split"):
	        instance_obj.wait_all()

        # Marking for output
        self.sample_data["outputs"] = ["%s/out.bam" % self.config.general.temp_dir]

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
