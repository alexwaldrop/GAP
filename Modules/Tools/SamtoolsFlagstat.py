from Modules import Module

class SamtoolsFlagstat(Module):
    def __init__(self, module_id):
        super(SamtoolsFlagstat, self).__init__(module_id)

        self.output_keys = ["flagstat"]

    def define_input(self):
        self.add_argument("bam",        is_required=True)
        self.add_argument("bam_idx",    is_required=True)
        self.add_argument("samtools",   is_required=True, is_resource=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=1)
        self.add_argument("mem",        is_required=True, default_value=1)

    def define_output(self):
        # Declare bam index output filename
        flagstat = self.generate_unique_file_name(".flagstat.out")
        self.add_output("flagstat", flagstat, is_path=True)

    def define_command(self):
        # Define command for running samtools index from a platform
        bam         = self.get_argument("bam")
        samtools    = self.get_argument("samtools")
        flagstat    = self.get_output("flagstat")

        # Generating Flagstat command
        cmd = "%s flagstat %s > %s" % (samtools, bam, flagstat)
        return cmd