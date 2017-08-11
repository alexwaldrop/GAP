from Modules import Module

class SamtoolsFlagstat(Module):
    def __init__(self, module_id):
        super(SamtoolsFlagstat, self).__init__(module_id)

        self.input_keys = ["bam", "bam_idx", "samtools", "nr_cpus", "mem"]
        self.output_keys = ["flagstat"]

        # Command should be run on main processor
        self.quick_command = True

    def define_input(self):
        self.add_argument("bam",        is_required=True)
        self.add_argument("bam_idx",    is_required=True)
        self.add_argument("samtools",   is_required=True, is_resource=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=1)
        self.add_argument("mem",        is_required=True, default_value=1)

    def define_output(self, platform, split_name=None):
        # Declare bam index output filename
        flagstat = self.generate_unique_file_name(split_name=split_name, extension=".flagstat.out")
        self.add_output(platform, "flagstat", flagstat)

    def define_command(self, platform):
        # Define command for running samtools index from a platform
        bam         = self.get_arguments("bam").get_value()
        samtools    = self.get_arguments("samtools").get_value()
        flagstat    = self.get_output("flagstat")

        # Generating Flagstat command
        cmd = "%s flagstat %s > %s" % (samtools, bam, flagstat)
        return cmd