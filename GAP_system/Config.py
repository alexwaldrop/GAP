import os.path
import glob
from GAP_interfaces import Main

class Config(Main):

    class General():
        def __init__(self):
            self.config_file    = ""
            self.project_name   = ""
            self.verbosity      = -1
            self.goal           = ""

    class Paths():
        def __init__(self):
            self.R1         = ""
            self.R2         = ""
            
            self.bwa        = ""

    class Cluster():
        def __init__(self):
            self.ID         = -1
            self.instances  = -1
            self.partition  = ""
            self.cpus       = -1
            self.mem        = ""

    class Aligner():
        def __init__(self):
            self.ID         = -1
            self.ref        = ""

    def __init__(self, config_file):        
        # Initializing the data
        self.general    = self.General()
        self.paths      = self.Paths()
        self.cluster    = self.Cluster()
        self.aligner    = self.Aligner()

        self.general.goals  = ["align"]
        self.cluster.types  = ["SLURM"]
        self.aligner.types  = ["BWA"]
        
        # Reading config file
        self.readConfigFile(config_file)

    def readConfigFile(self, config_file):

        # Checking if the config file exists
        if not os.path.isfile(config_file):
            self.error("Config file not found!")
        
        self.general.config_file    = config_file

        # Extracting data from config file
        cursor_location = ""

        with open(config_file, "r") as inp:
            for line in inp:
                if line[0] == "#":
                    continue
 
                if "[general]" in line:
                    cursor_location = "general"
                    continue

                elif "[paths]" in line:
                    cursor_location = "paths"
                    continue

                elif "[cluster]" in line:
                    cursor_location = "cluster"
                    continue            
    
                elif "[aligner]" in line:
                    cursor_location = "aligner"
                    continue                

                data = line.strip("\n").split("=")
                data = [x.strip(" ") for x in data]

                # [general] section
                if cursor_location == "general":
                    if data[0] == "project_name":
                        self.general.project_name = data[1]
    
                    if data[0] == "verbosity":
                        try:
                            self.general.verbosity = int(data[1])
                        except:
                            self.error("In config file, [general]:verbosity should be an integer!")

                    if data[0] == "goal":
                        if data[1] in self.general.goals:
                            self.general.goal = data[1]
                        else:
                            self.error("In config file, [general]:goal not recognized!")
                            
                # [paths] section
                if cursor_location == "paths":
                    if data[0] == "bwa":
                        self.paths.bwa = data[1]
                        
                        if not os.path.isfile(self.paths.bwa):
                            self.error("BWA path not found!")

                    if data[0] == "R1":
                        self.paths.R1 = data[1]

                        if not os.path.isfile(self.paths.R1):
                            self.error("Sample R1 file not found!")

                    if data[0] == "R2":
                        self.paths.R2 = data[1]
    
                        if not os.path.isfile(self.paths.R2):
                            self.error("Sample R2 file not found!")

                # [cluster] section
                if cursor_location == "cluster":
                    if data[0] == "ID":
                        try:
                            self.cluster.ID = int(data[1])
                        except:
                            self.error("In config file, [cluster]:ID should be an integer!")

                        if self.cluster.ID >= len(self.cluster.types):
                            self.error("In config file, [cluster]:ID is out of range!")
                        
                    if data[0] == "nodes":
                        try:
                            self.cluster.nodes = int(data[1])
                        except:
                            self.error("In config file, [cluster]:nodes should be an integer!")

                    if data[0] == "partition":
                        self.cluster.partition = data[1]

                        if len(self.cluster.partition) == 0:
                            self.error("In config file, [cluster]:partition, no partitions are specified!")

                    if data[0] == "mincpus":
                        try:
                            self.cluster.mincpus = int(data[1])
                        except:
                            self.error("In config file, [cluster]:mincpus should be an integer!")

                    if data[0] == "mem":
                        self.cluster.mem = data[1]

                        if len(self.cluster.mem) == 0:
                            self.error("In config file, [cluster]:mem, no memory limitation is specified!")
                
                # [aligner] section
                if cursor_location == "aligner":
                    if data[0] == "ID":
                        try:
                            self.aligner.ID = int(data[1])
                        except:
                            self.error("In config file, [aligner]:ID should be an integer!")

                    if data[0] == "ref":
                        self.aligner.ref = data[1]
                        if len(glob.glob(self.aligner.ref + "*")) == 0:
                            self.error("In config file, [aligner]:ref, the reference path does not exist!")

    def validate(self):
        pass
