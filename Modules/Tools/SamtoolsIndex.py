from Modules import Module

class SamtoolsIndex(Module):
    def __init__(self, module_id):
        super(SamtoolsIndex, self).__init__(module_id)

        self.input_keys = ["bam", "samtools", "nr_cpus", "mem"]
        self.output_keys = ["bam_idx"]

        # Command should be run on main processor
        self.quick_command = True

    def define_input(self):
        self.add_argument("bam",        is_required=True)
        self.add_argument("samtools",   is_required=True, is_resource=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=1)
        self.add_argument("mem",        is_required=True, default_value=1)

    def define_output(self, platform, split_name=None):

        # Get arguments value
        bams = self.get_arguments("bam").get_value()

        # Check if the input is a list
        if isinstance(bams, list):
            bams_idx = [bam + ".bai" for bam in bams]
        else:
            bams_idx = bams + ".bai"

        # Add new bams as output
        self.add_output(platform, "bam_idx", bams_idx, is_path=False)

    def define_command(self, platform):
        # Define command for running samtools index from a platform
        bam         = self.get_arguments("bam").get_value()
        samtools    = self.get_arguments("samtools").get_value()
        bam_idx     = self.get_output("bam_idx")

        # Generating indexing command
        cmd = ""
        if isinstance(bam, list):
            for b_in, b_out in zip(bam, bam_idx):
                cmd += "%s index %s %s !LOG3! & " % (samtools, b_in, b_out)
            cmd += "wait"
        else:
            cmd = "%s index %s %s !LOG3!" % (samtools, bam, bam_idx)
        return cmd
