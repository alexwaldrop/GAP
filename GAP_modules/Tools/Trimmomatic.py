from GAP_interfaces import Tool

__main_class__ = "Trimmomatic"

class Trimmomatic(Tool):

    def __init__(self, config, sample_data):
        super(Trimmomatic, self).__init__(config, sample_data)

        self.is_phred33         = self.sample_data["phred33"]

        self.temp_dir           = self.config["paths"]["instance_tmp_dir"]

        self.can_split      = False

        self.nr_cpus        = self.config["platform"]["MS_nr_cpus"]
        self.mem            = self.config["platform"]["MS_mem"]

        self.input_keys     = ["R1", "R2"]
        self.output_keys    = ["R1", "R1_unpair", "R2", "R2_unpair", "trim_report"]

        self.req_tools      = ["trimmomatic", "java"]
        self.req_resources  = ["adapters"]

        self.R1             = None
        self.R2             = None

    def get_command(self, **kwargs):

        # Obtaining the arguments
        self.R1                 = kwargs.get("R1",              None)
        self.R2                 = kwargs.get("R2",              None)
        self.nr_cpus            = kwargs.get("nr_cpus",         self.nr_cpus)
        self.mem                = kwargs.get("mem",             self.mem)

        # Generating variables
        R1_pair     = "%s/R1_%s_trimmed.fastq" % (self.temp_dir, self.sample_data["sample_name"])
        R1_unpair   = "%s/R1_%s_trimmed_unpaired.fastq" % (self.temp_dir, self.sample_data["sample_name"])
        R2_pair     = "%s/R2_%s_trimmed.fastq" % (self.temp_dir, self.sample_data["sample_name"])
        R2_unpair   = "%s/R2_%s_trimmed_unpaired.fastq" % (self.temp_dir, self.sample_data["sample_name"])
        out_file    = "%s/%s_trimmed_output.txt" % (self.temp_dir, self.sample_data["sample_name"])
        steps       = [ "ILLUMINACLIP:%s:2:20:7:1:true" % self.resources["adapters"],
                        "LEADING:5",
                        "TRAILING:5",
                        "SLIDINGWINDOW:4:10",
                        "MINLEN:36" ]
        phred       = "-phred33" if self.is_phred33 else "-phred64"
        jvm_options = "-Xmx%dG -Djava.io.tmp=%s" % (self.mem*4/5, self.temp_dir)

        # Generating command
        trim_cmd = "%s %s -jar %s PE -threads %d %s %s %s %s %s %s %s %s > %s 2>&1" % (
            self.tools["java"], jvm_options, self.tools["trimmomatic"], self.nr_cpus, phred, self.R1, self.R2,
            R1_pair, R1_unpair, R2_pair, R2_unpair, " ".join(steps), out_file)

        # Change the input data to correct one
        self.output = dict()
        self.output["R1"] = R1_pair
        self.output["R1_unpair"] = R1_unpair
        self.output["R2"] = R2_pair
        self.output["R2_unpair"] = R2_unpair
        self.output["trim_report"] = out_file

        return trim_cmd
