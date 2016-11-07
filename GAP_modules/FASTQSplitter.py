from GAP_interfaces import Main

__main_class__ = "FASTQSplitter"

class FASTQSplitter(Main):

    def __init__(self, config, sample_data):
        Main.__init__(self, config)

        self.config = config
        self.sample_data = sample_data

        self.temp_dir = self.config["general"]["temp_dir"]

        self.prefix = ["fastq_R1", "fastq_R2"]

        self.R1          = None
        self.R2          = None
        self.nr_splits   = None
        self.output_path = None

    def get_output(self):
        return self.output_path

    def get_command(self, **kwargs):

        # Obtaining the arguments
        self.R1             = kwargs.get("R1",          self.sample_data["R1"])
        self.R2             = kwargs.get("R2",          self.sample_data["R2"])
        self.nr_splits      = kwargs.get("nr_splits",   2)

        # Validate arguments
        self.validate()

        # Generating the commands
        cmds = list()

        # Obtaining number of reads
        cmds.append("nr_lines=$(( `du -b %s | cut -f1` / `head -n4 %s | wc -c` / %d * 4))" % (self.R1, self.R1, self.nr_splits) )

        self.output_path = [ { "R1" : "%s/%s_%02d" % (self.temp_dir, self.prefix[0], i),
                               "R2" : "%s/%s_%02d" % (self.temp_dir, self.prefix[1], i) } for i in xrange(self.nr_splits)]

        # Splitting the files
        for prefix in self.prefix:
            if "R1" in prefix:
                cmds.append("split --suffix-length=2 --numeric-suffixes --lines=$nr_lines %s %s/%s_" % (self.R1, self.temp_dir, prefix) )
            else:
                cmds.append("split --suffix-length=2 --numeric-suffixes --lines=$nr_lines %s %s/%s_" % (self.R2, self.temp_dir, prefix) )

        return " && ".join(cmds)

    def validate(self):
        pass
