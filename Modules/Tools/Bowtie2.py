from Modules import Module

class Bowtie2(Module):
    def __init__(self, module_id):
        super(Bowtie2, self).__init__(module_id)

        self.input_keys = ["R1", "R2", "bowtie2", "ref", "nr_cpus", "mem"]

        self.output_keys = ["sam", "R1", "R2"]

    def define_input(self):
        self.add_argument("R1",             is_required=True)
        self.add_argument("R2")
        self.add_argument("bowtie2",        is_required=True, is_resource=True)
        self.add_argument("ref",            is_required=True, is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=8)
        self.add_argument("mem",            is_required=True, default_value="nr_cpus * 2")

    def define_output(self, platform, split_name=None):

        # Declare sam and unmapped FASTQ output file
        sam_out                 = self.generate_unique_file_name(split_name=split_name,
                                                                 extension="sam")
        R1_unmapped_fastq_out   = self.generate_unique_file_name(split_name=split_name,
                                                                 extension="unmapped.1.fastq.gz")

        self.add_output(platform, "sam", sam_out)
        self.add_output(platform, "R1", R1_unmapped_fastq_out)

        if self.get_arguments("R2").get_value() is not None:
            R2_unmapped_fastq_out = self.generate_unique_file_name(split_name=split_name,
                                                                   extension="unmapped.2.fastq.gz")
            self.add_output(platform, "R2", R2_unmapped_fastq_out)

    def define_command(self, platform):

        # Get arguments to run Bowtie2 aligner
        R1                  = self.get_arguments("R1").get_value()
        R2                  = self.get_arguments("R2").get_value()
        bowtie2             = self.get_arguments("bowtie2").get_value()
        ref                 = self.get_arguments("ref").get_value()
        nr_cpus             = self.get_arguments("nr_cpus").get_value()
        sam_out             = self.get_output("sam")
        r1_unmapped_fastq   = self.get_output("R1")
        unmapped_fastq_base = r1_unmapped_fastq.replace(".1.", ".%.")

        # Design command line based on read type (i.e. paired-end or single-end)
        if self.get_arguments("R2").get_value() is not None:
            bowtie2_cmd = "%s --local -q -p %s -x %s -1 %s -2 %s -S %s --no-mixed --no-discordant --un-conc-gz %s -t !LOG2!" % \
                          (bowtie2, nr_cpus, ref, R1, R2, sam_out, unmapped_fastq_base)
        else:
            bowtie2_cmd = "%s --local -q -p %s -x %s -U %s -S %s --no-mixed --no-discordant --al-gz %s -t !LOG2!" % \
                          (bowtie2, nr_cpus, ref, R1, sam_out, r1_unmapped_fastq)

        cmd = "%s" % bowtie2_cmd
        return cmd