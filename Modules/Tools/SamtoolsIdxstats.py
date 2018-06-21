from Modules import Module

class SamtoolsIdxstats(Module):
    def __init__(self, module_id, is_docker = False):
        super(SamtoolsIdxstats, self).__init__(module_id, is_docker)
        self.output_keys = ["idxstats"]

    def define_input(self):
        self.add_argument("bam",        is_required=True)
        self.add_argument("bam_idx",    is_required=True)
        self.add_argument("samtools",   is_required=True, is_resource=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=1)
        self.add_argument("mem",        is_required=True, default_value=1)

    def define_output(self):
        # Declare bam idxstats output filename
        idxstats = self.generate_unique_file_name(extension=".idxstats.out")
        self.add_output("idxstats", idxstats)

    def define_command(self):
        # Define command for running samtools idxstats from a platform
        bam         = self.get_argument("bam")
        samtools    = self.get_argument("samtools")

        idxstats    = self.get_output("idxstats")

        # Generating Idxstats command
        cmd = "{0} idxstats {1} > {2}".format(samtools, bam, idxstats)
        return cmd