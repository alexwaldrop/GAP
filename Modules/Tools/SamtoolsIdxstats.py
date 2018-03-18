from Modules import Module

class SamtoolsIdxstats(Module):
    def __init__(self, module_id):
        super(SamtoolsIdxstats, self).__init__(module_id)

        self.input_keys = ["bam", "bam_idx", "samtools", "nr_cpus", "mem"]
        self.output_keys = ["idxstats"]

        # Command should be run on main processor
        self.quick_command = True

    def define_input(self):
        self.add_argument("bam",        is_required=True)
        self.add_argument("bam_idx",    is_required=True)
        self.add_argument("samtools",   is_required=True, is_resource=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=1)
        self.add_argument("mem",        is_required=True, default_value=1)

    def define_output(self, platform, split_name=None):
        # Declare bam idxstats output filename
        idxstats = self.generate_unique_file_name(split_name=split_name, extension=".idxstats.out")
        self.add_output(platform, "idxstats", idxstats)

    def define_command(self, platform):
        # Define command for running samtools idxstats from a platform
        bam         = self.get_arguments("bam").get_value()
        samtools    = self.get_arguments("samtools").get_value()
        idxstats    = self.get_output("idxstats")

        # Generating Idxstats command
        cmd = "%s idxstats %s > %s" % (samtools, bam, idxstats)
        return cmd