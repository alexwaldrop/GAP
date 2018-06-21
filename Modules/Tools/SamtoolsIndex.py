from Modules import Module

class SamtoolsIndex(Module):
    def __init__(self, module_id, is_docker = False):
        super(SamtoolsIndex, self).__init__(module_id, is_docker)
        self.output_keys = ["bam_idx"]

    def define_input(self):
        self.add_argument("bam",        is_required=True)
        self.add_argument("samtools",   is_required=True, is_resource=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=1)
        self.add_argument("mem",        is_required=True, default_value=1)

    def define_output(self):

        # Get arguments value
        bams = self.get_argument("bam")

        # Check if the input is a list
        if isinstance(bams, list):
            bams_idx = [bam + ".bai" for bam in bams]
        else:
            bams_idx = bams + ".bai"

        # Add new bams as output
        self.add_output("bam_idx", bams_idx, is_path=True)

    def define_command(self):
        # Define command for running samtools index from a platform
        bam         = self.get_argument("bam")
        samtools    = self.get_argument("samtools")
        bam_idx     = self.get_output("bam_idx")

        # Generating indexing command
        cmd = ""
        if isinstance(bam, list):
            for b_in, b_out in zip(bam, bam_idx):
                cmd += "{0} index {1} {2} !LOG3! & ".format(samtools, b_in, b_out)
            cmd += "wait"
        else:
            cmd = "{0} index {1} {2} !LOG3!".format(samtools, bam, bam_idx)
        return cmd
