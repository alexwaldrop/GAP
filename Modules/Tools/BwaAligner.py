from Modules import Module

class BwaAligner(Module):
    def __init__(self, module_id, is_docker = False):
        super(BwaAligner, self).__init__(module_id, is_docker)
        self.output_keys = ["bam", "bam_sorted"]

    def define_input(self):
        self.add_argument("R1",             is_required=True)
        self.add_argument("R2")
        self.add_argument("bwa",            is_required=True, is_resource=True)
        self.add_argument("samtools",       is_required=True, is_resource=True)
        self.add_argument("ref",            is_required=True, is_resource=True)
        self.add_argument("read_group",     is_required=True)
        self.add_argument("nr_cpus",        is_required=True, default_value="max")
        self.add_argument("mem",            is_required=True, default_value="max(nr_cpus * 1.5, 20)")

    def define_output(self):
        # Declare bam output file
        bam_out = self.generate_unique_file_name(extension=".sorted.bam")
        self.add_output("bam", bam_out)
        # Declare that bam is sorted
        self.add_output("bam_sorted", True, is_path=False)

    def define_command(self):
        # Get arguments to run BWA aligner
        R1              = self.get_argument("R1")
        R2              = self.get_argument("R2")
        ref             = self.get_argument("ref")
        rg_header       = self.get_argument("read_group")
        nr_cpus         = self.get_argument("nr_cpus")
        bam_out         = self.get_output("bam")
        bwa = self.get_argument("bwa")
        samtools = self.get_argument("samtools")

        if R2 is not None:
            # Generate bwa-mem paired-end command
            align_cmd = '{0} mem -M -R "{1}" -t {2} {3} {4} {5} !LOG2!'.format(
                bwa, rg_header, nr_cpus, ref, R1, R2)
        else:
            # Generate bwa-mem single-end command
            align_cmd = '{0} mem -M -R "{1}" -t {2} {3} {4} !LOG2!'.format(
                bwa, rg_header, nr_cpus, ref, R1)

        # Generating command for converting SAM to BAM
        sam_to_bam_cmd = "{0} view -uS -@ {1} - !LOG2!".format(samtools, nr_cpus)

        # Generating command for sorting BAM
        bam_sort_cmd = "{0} sort -@ {1} - -o {2} !LOG3!".format(samtools, nr_cpus, bam_out)

        cmd = "{0} | {1} | {2}".format(align_cmd, sam_to_bam_cmd, bam_sort_cmd)
        return cmd
