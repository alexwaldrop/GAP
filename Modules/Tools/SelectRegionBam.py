from Modules import Module

class SelectRegionBam(Module):
    def __init__(self, module_id):
        super(SelectRegionBam, self).__init__(module_id)

        self.input_keys = ["bam", "bam_idx", "regions", "samtools", "nr_cpus", "mem"]
        self.output_keys = ["bam"]

        # Command should be run on main processor
        self.quick_command = True

    def define_input(self):
        self.add_argument("bam",        is_required=True)
        self.add_argument("bam_idx",    is_required=True)
        self.add_argument("regions",    is_required=True)
        self.add_argument("samtools",   is_required=True, is_resource=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=1)
        self.add_argument("mem",        is_required=True, default_value=1)

    def define_output(self, platform, split_name=None):

        # Get arguments value
        bams = self.get_arguments("bam").get_value()

        # Check if the input is a list
        if isinstance(bams, list):
            new_bams = [bam.replace(".bam", ".subset.bam") for bam in bams]
        else:
            new_bams = bams.replace(".bam", ".subset.bam")

        # Add new bams as output
        self.add_output(platform, "bam", new_bams, is_path=False)

    def define_command(self, platform):
        # Define command for running samtools view from a platform
        bam         = self.get_arguments("bam").get_value()
        regions     = self.get_arguments("regions").get_value()
        samtools    = self.get_arguments("samtools").get_value()
        bam_out     = self.get_output("bam")

        # Generating the regions string
        if isinstance(regions, list):
            reg = " ".join(regions)
        else:
            reg = regions

        # Generating samtools view command
        cmd = ""
        if isinstance(bam, list):
            for b_in, b_out in zip(bam, bam_out):
                cmd += "%s view -b %s %s > %s !LOG2! & " % (samtools, b_in, reg, b_out)
            cmd += "wait"
        else:
            cmd = "%s view -b %s %s > %s !LOG2!" % (samtools, bam, reg, bam_out)
        return cmd
