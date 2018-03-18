from Modules import Module

class SelectUnmappedBam(Module):
    def __init__(self, module_id):
        super(SelectUnmappedBam, self).__init__(module_id)

        self.input_keys = ["bam", "samtools", "picard", "java","nr_cpus", "mem"]
        self.output_keys = ["R1", "R2"]

        # Command should be run on main processor
        self.quick_command = True

    def define_input(self):
        self.add_argument("bam",        is_required=True)
        self.add_argument("samtools",   is_required=True, is_resource=True)
        self.add_argument("picard",     is_required=True, is_resource=True)
        self.add_argument("java",       is_required=True, is_resource=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=6)
        self.add_argument("mem",        is_required=True, default_value=10)

    def define_output(self, platform, split_name=None):

        # Generate R1 reads
        R1 = self.generate_unique_file_name(split_name=split_name, extension=".R1.fastq.gz")
        self.add_output(platform, "R1", R1)

        # Generate R2 reads
        R2 = self.generate_unique_file_name(split_name=split_name, extension=".R2.fastq.gz")
        self.add_output(platform, "R2", R2)

    def define_command(self, platform):
        # Obtain the arguments data
        bam         = self.get_arguments("bam").get_value()
        samtools    = self.get_arguments("samtools").get_value()
        picard      = self.get_arguments("picard").get_value()
        nr_cpus     = self.get_arguments("nr_cpus").get_value()
        java        = self.get_arguments("java").get_value()

        # Get the output keys
        R1      = self.get_output("R1")
        R2      = self.get_output("R2")

        # Extract unaligned reads
        cmd1 = "%s view -@ %s -f 4 -bu %s !LOG2!" % (samtools, nr_cpus, bam)

        # Extract only reads that have their mate unaligned as well
        cmd2 = "%s view -@ %s -f 8 -bu - !LOG2!" % (samtools, nr_cpus)

        # Convert to FASTQ the final values
        cmd3 = "%s -jar %s SamToFastq I=/dev/stdin F=%s F2=%s VALIDATION_STRINGENCY=LENIENT !LOG3!" \
               % (java, picard, R1, R2)

        return " %s | %s | %s" % (cmd1, cmd2, cmd3)
