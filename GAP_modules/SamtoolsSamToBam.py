from GAP_interfaces import Converter

class SamtoolsSamToBam(Converter):
    
    def __init__(self, config):
        Converter.__init__(self, config)

        self.config = config

        self.samtools = self.config["paths"]["samtools"]

        self.output_path = None

    def get_output(self):
        return self.output_path

    def get_command(self, **kwargs):

        # Obtaining the arguments
        self.threads        = kwargs.get("cpus",        self.config["instance"]["nr_cpus"])

        self.validate()

        return "%s view -bS -@ %d -" % (self.samtools, self.threads)

    def validate(self):
        if self.threads == -1:
            self.error("In converter implementation, number of threads not specified!")
