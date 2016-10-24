from GAP_interfaces import Sorter

class SamtoolsBAMSorter(Sorter):
    
    def __init__(self, config):
        Sorter.__init__(self, config)

        self.config = config

        self.samtools       = self.config.paths.samtools

        self.temp_dir       = config.general.temp_dir

        self.prefix         = None
        self.threads        = None
        self.output_path    = None

    def get_output(self):
        return self.output_path

    def get_command(self, **kwargs):

        self.prefix         = kwargs.get("prefix",      "")
        self.threads        = kwargs.get("cpus",        self.config.general.nr_cpus)

        self.validate()

        self.output_path = "%s/%s.bam" % (self.temp_dir, self.prefix)

        return "%s sort -@ %d - %s/%s" % (self.samtools, self.threads, self.temp_dir, self.prefix)

    def validate(self):
        if self.threads  == -1:
            self.error("In sorter implementation, number of threads not specified!")

        if self.prefix == "":
            self.error("In sorter implementation, the prefix is not specified!")
