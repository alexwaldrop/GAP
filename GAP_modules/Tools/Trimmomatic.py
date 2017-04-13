from GAP_interfaces import Tool

__main_class__ = "Trimmomatic"

class Trimmomatic(Tool):

    def __init__(self, config, sample_data):
        super(Trimmomatic, self).__init__()

        self.config = config
        self.sample_data = sample_data

        self.trimmomatic_jar    = self.config["paths"]["trimmomatic"]
        self.adapters           = self.config["paths"]["adapters"]
        self.is_phred33         = self.sample_data["phred33"]

        self.temp_dir           = self.config["general"]["temp_dir"]

        self.can_split      = False

        self.R1             = None
        self.R2             = None
        self.threads        = None

    def get_command(self, **kwargs):

        # Obtaining the arguments
        self.R1                 = kwargs.get("R1",              self.sample_data["R1"])
        self.R2                 = kwargs.get("R2",              self.sample_data["R2"])
        self.threads            = kwargs.get("cpus",            self.config["instance"]["nr_cpus"])

        # Generating variables
        R1_pair     = "%s/R1_%s_trimmed.fastq" % (self.temp_dir, self.sample_data["sample_name"])
        R1_unpair   = "%s/R1_%s_trimmed_unpaired.fastq" % (self.temp_dir, self.sample_data["sample_name"])
        R2_pair     = "%s/R2_%s_trimmed.fastq" % (self.temp_dir, self.sample_data["sample_name"])
        R2_unpair   = "%s/R2_%s_trimmed_unpaired.fastq" % (self.temp_dir, self.sample_data["sample_name"])
        steps       = [ "ILLUMINACLIP:%s:2:30:7" % self.adapters,
                        "LEADING:5",
                        "TRAILING:5",
                        "SLIDINGWINDOW:4:20",
                        "MINLEN:36" ]
        phred       = "-phred33" if self.is_phred33 else "-phred64"

        # Generating command
        trim_cmd = "java -jar %s PE -threads %d %s %s %s %s %s %s %s %s !LOG3!" % (self.trimmomatic_jar, self.threads, phred, self.R1, self.R2, R1_pair, R1_unpair, R2_pair, R2_unpair, " ".join(steps))

        # Change the input data to correct one
        self.sample_data["R1_untrim"] = self.sample_data["R1"]
        self.sample_data["R2_untrim"] = self.sample_data["R2"]
        self.sample_data["R1"] = R1_pair
        self.sample_data["R2"] = R2_pair

        return trim_cmd
