import importlib
from GAP_interfaces import Main

class Node(Main):

    def __init__(self, config, platform, sample_data, module_name):

        Main.__init__(self, config)

        self.config = config
        self.platform = platform
        self.sample_data = sample_data

#        try:
        self.main = self.initialize_module(module_name)
        self.instance   = self.main["class"](self.config)
#        except:
#            self.error("Module %s cannot be imported!" % module_name)
        
        self.process = None

    def initialize_module(self, module_name):

        d = {}
        d["module_name"]    = module_name
        d["module"]         = importlib.import_module("GAP_modules.%s" % d["module_name"])
        d["class_name"]     = d["module"].__main_class__
        d["class"]          = d["module"].__dict__[ d["class_name"] ] 

        return d

    def run(self):

        self.instance.R1 = self.sample_data["R1_new_path"]
        self.instance.R2 = self.sample_data["R2_new_path"]
        self.instance.threads     = 32

        cmd = self.instance.getCommand()

        self.process = self.platform.runCommand("align", cmd, on_instance=self.platform.main_server)

        self.sample_data["outputs"] = ["/data/out.bam"]

        self.process.wait()

    def validate(self):
        pass
