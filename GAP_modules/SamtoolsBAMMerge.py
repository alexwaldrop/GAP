from GAP_interfaces import Main

__main_class__ = "SamtoolsBAMMerge"

class SamtoolsBAMMerge(Main):

    def __init__(self, config, sample_data):
        Main.__init__(self, config)

        self.config = config
        self.sample_data = sample_data

        self.samtools     = self.config.paths.samtools

        self.temp_dir     = self.config.general.temp_dir

        self.sample_name  = self.sample_data["sample_name"]

        self.threads      = None
        self.inputs       = None
        self.nr_splits    = None
        self.sorted_input = None
        self.output_path  = None

    def get_output(self):
        return self.output_path

    def get_command(self, **kwargs):

        # Obtaining the arguments
        self.threads        = kwargs.get("cpus",            self.config.general.nr_cpus)
        self.nr_splits      = kwargs.get("nr_splits",       2)
        self.sorted_input   = kwargs.get("sorted_input",    True)

        bam_splits = ["%s/%s_%d.bam" % (self.temp_dir, self.sample_name, i) for i in range(self.nr_splits)]
        self.inputs         = kwargs.get("inputs",          bam_splits)

        # Validate the arguments
        self.validate()

        self.output_path = "%s/%s.bam" % (self.temp_dir, self.sample_name)

        if self.sorted_input:
            return "%s merge -@%d %s %s" % (self.samtools, self.threads, self.output_path, " ".join(bam_splits))
        else:
            return "%s cat -o %s %s" % (self.samtools, self.output_path, " ".join(bam_splits))

    def validate(self):
        if self.threads == -1:
            self.error("In merger implementation, the number of threads is not specified!")

        if self.nr_splits == 0:
            self.error("Number of splits was not set before merging!")
