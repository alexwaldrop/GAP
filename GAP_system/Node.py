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
        self.process = self.platform.runCommand("split", cmd, on_instance=self.platform.main_server)
        self.process.wait()

        # Creating the split servers
        self.platform.createSplitServers(self.config.general.nr_splits, nr_cpus=self.config.general.nr_cpus, is_preemptible=False)

        cmds = []

        # Moving the splits in their folders
        for split_id in xrange(self.config.general.nr_splits):
            cmds.append("mv %s/fastq_R1_%02d %s/split%d/" % (self.config.general.temp_dir, split_id, self.config.general.temp_dir, split_id) )
            cmds.append("mv %s/fastq_R2_%02d %s/split%d/" % (self.config.general.temp_dir, split_id, self.config.general.temp_dir, split_id) )

        self.process = self.platform.runCommand("move_splits", " && ".join(cmds), on_instance=self.platform.main_server)
        self.process.wait()

        # Calling the process on each split
        procs = []
        for split_id in xrange(self.config.general.nr_splits):
            self.main["instance"].R1 = "%s/fastq_R1_%02d" % (self.config.general.temp_dir, split_id)
            self.main["instance"].R2 = "%s/fastq_R2_%02d" % (self.config.general.temp_dir, split_id)
            self.main["instance"].threads = self.config.general.nr_cpus

            cmd = self.main["instance"].getCommand()

            procs.append(self.platform.runCommand("align%d" % split_id, cmd, on_instance="split%d-server" % split_id) )

        # Waiting for all the split aligning processes to finish
        while not all( proc.poll() is not None for proc in procs ):
            time.sleep(5)

        # Setting up the merger
        self.merge["instance"].nr_splits = self.config.general.nr_splits
        self.merge["instance"].threads = self.config.general.nr_cpus

        # Running the merger
        cmd = self.merge["instance"].getCommand()
        self.process = self.platform.runCommand("merge", cmd, on_instance=self.platform.main_server)
        self.process.wait()

        # Destroying the splits
        procs = []
        for split_id in xrange(self.config.general.nr_splits):
            procs.append( self.platform.destroyInstance("split%d-server" % split_id) )

        # Waiting for all the split servers to DIE!!!
        while not all( proc.poll() is not None for proc in procs ):
            time.sleep(5)

        # Marking for output
        self.sample_data["outputs"] = ["/data/out.bam"]

    def run_normal(self):

        self.main["instance"].R1 = self.sample_data["R1_new_path"]
        self.main["instance"].R2 = self.sample_data["R2_new_path"]
        self.main["instance"].threads = self.config.general.nr_cpus

        cmd = self.main["instance"].getCommand()

        self.process = self.platform.runCommand("align", cmd, on_instance=self.platform.main_server)

        self.sample_data["outputs"] = ["/data/out.bam"]

        self.process.wait()

    def run(self):

        if self.main["instance"].can_split and self.config.general.split:
            self.run_split()
        else:
            self.run_normal()

    def validate(self):
        pass
