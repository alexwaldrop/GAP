import logging

from Modules import Module

class BwaAligner(Module):
    def __init__(self, module_id):
        super(BwaAligner, self).__init__(module_id)

        self.input_keys = ["R1", "R2", "bwa", "samtools", "ref", "sample_name",
                           "lib_name", "seq_platform", "nr_cpus", "mem"]

        self.output_keys = ["bam", "bam_sorted"]

    def define_input(self):
        self.add_argument("R1",             is_required=True)
        self.add_argument("R2")
        self.add_argument("bwa",            is_required=True, is_resource=True)
        self.add_argument("samtools",       is_required=True, is_resource=True)
        self.add_argument("ref",            is_required=True, is_resource=True)
        self.add_argument("sample_name",    is_required=True)
        self.add_argument("lib_name",       is_required=True)
        self.add_argument("seq_platform",   is_required=True, default_value="Illumina")
        self.add_argument("nr_cpus",        is_required=True, default_value="max")
        self.add_argument("mem",            is_required=True, default_value="max(nr_cpus * 1.5, 20)")

    def define_output(self, platform, split_name=None):
        # Declare bam output file
        bam_out = self.generate_unique_file_name(split_name=split_name, extension=".sorted.bam")
        self.add_output(platform, "bam", bam_out)

        # Declare that bam is sorted
        self.add_output(platform, "bam_sorted", True, is_path=False)

    def define_command(self, platform):
        # Get arguments to run BWA aligner
        R1              = self.get_arguments("R1").get_value()
        R2              = self.get_arguments("R2").get_value()
        bwa             = self.get_arguments("bwa").get_value()
        samtools        = self.get_arguments("samtools").get_value()
        ref             = self.get_arguments("ref").get_value()
        sample_name     = self.get_arguments("sample_name").get_value()
        lib_name        = self.get_arguments("lib_name").get_value()
        seq_platform    = self.get_arguments("seq_platform").get_value()
        nr_cpus         = self.get_arguments("nr_cpus").get_value()
        bam_out         = self.get_output("bam")

        # Get read group header
        try:
            logging.info("Attempting to determine read group header for fastq file: %s" % R1)
            rg_header = BwaAligner.__get_rg_header(platform, R1, sample_name, lib_name, seq_platform)
        except:
            logging.error("Module BwaAligner unable to determine read group header!")
            raise

        if R2 is not None:
            # Generate bwa-mem paired-end command
            align_cmd = "%s mem -M -R \"%s\" -t %d %s %s %s !LOG2!" % \
                          (bwa, rg_header, nr_cpus, ref, R1, R2)
        else:
            # Generate bwa-mem single-end command
            align_cmd = "%s mem -M -R \"%s\" -t %d %s %s !LOG2!" % \
                          (bwa, rg_header, nr_cpus, ref, R1)

        # Generating command for converting SAM to BAM
        sam_to_bam_cmd = "%s view -uS -@ %d - !LOG2!" % (samtools, nr_cpus)

        # Generating command for sorting BAM
        bam_sort_cmd = "%s sort -@ %d - -o %s !LOG3!" % (samtools, nr_cpus, bam_out)

        cmd = "%s | %s | %s" % (align_cmd, sam_to_bam_cmd, bam_sort_cmd)
        return cmd

    @staticmethod
    def __get_rg_header(platform, R1, sample_name, lib_name, seq_platform):

        # Obtain the read header
        if R1.endswith(".gz"):
            cmd = "zcat %s | head -n 1" % R1
        else:
            cmd = "head -n 1 %s" % R1
        out, err = platform.run_quick_command("fastq_header", cmd)

        # Generating the read group information
        fastq_header_data = out.lstrip("@").strip("\n").split(":")
        rg_id = ":".join(fastq_header_data[0:4])  # Read Group ID
        rg_pu = fastq_header_data[-1]  # Read Group Platform Unit
        rg_sm = sample_name if not isinstance(sample_name, list) else sample_name[0]    # Read Group Sample
        rg_lb = lib_name if not isinstance(lib_name, list) else lib_name[0]             # Read Group Library ID
        rg_pl = seq_platform if not isinstance(seq_platform, list) else seq_platform[0] # Read Group Platform used

        read_group_header = "\\t".join(["@RG", "ID:%s" % rg_id, "PU:%s" % rg_pu,
                                        "SM:%s" % rg_sm, "LB:%s" % rg_lb, "PL:%s" % rg_pl])

        # Generating the read group header
        return read_group_header
