from Module import Module

class SamtoolsIndex(Module):
    def __init__(self, module_id):
        super(SamtoolsIndex, self).__init__(module_id)

        self.input_keys = ["bam", "samtools", "nr_cpus", "mem"]
        self.output_keys = ["bam_idx"]

    def define_input(self):
        self.add_argument("bam", is_required=True)
        self.add_argument("samtools", is_required=True, is_resource=True)
        self.add_argument("nr_cpus", is_required=True, default_value=1)
        self.add_argument("mem", is_required=True, default_value=5)

    def define_output(self, platform, split_name=None):
        # Declare bam index output filename
        bam_idx = "%s.bai" % self.get_arguments("bam").get_value()
        self.add_output("bam_idx", bam_idx)

    def define_command(self, platform, split_name=None):
        # Define command for running samtools index from a platform
        bam         = self.get_arguments("bam").get_value()
        samtools    = self.get_arguments("samtools").get_value()
        bam_idx     = self.get_output("bam_idx").get_value()

        # Generating indexing command
        cmd = "%s index %s %s" % (samtools, bam, bam_idx)
        return cmd
