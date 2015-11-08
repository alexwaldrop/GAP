from GAP_interfaces import Main

class Task(Main):   

    def __init__(self, name, config, cluster):
        Main.__init__(self, config)
        self.name       = name
        self.type       = ""
        self.command    = ""
        self.requires   = []
        
        self.nodes      = 1
        self.mincpus    = 1

        self.job_id     = False
        self.which_node = -1

        self.cluster    = cluster

        self.is_done    = False

    def start(self):
        self.validate()       

        self.cluster.runCommand(self.name, self.command, nodes=self.nodes, mincpus=self.mincpus)
        while self.job_id == False:
            self.job_id = self.cluster.getJobID(self.name)

    def validate(self):
        if self.command == "":
            self.error("Task %s, has no command!" % self.name)

