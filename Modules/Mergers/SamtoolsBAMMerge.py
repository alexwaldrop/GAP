from Modules import Module

class SamtoolsBAMMerge(Module):

    def __init__(self, module_id):
        super(SamtoolsBAMMerge, self).__init__(module_id)

        self.input_keys   = ["bam", "bam_sorted", "samtools", "nr_cpus", "mem"]
        self.output_keys  = ["bam"]

    def define_input(self):
        self.add_argument("bam",            is_required=True)
        self.add_argument("bam_sorted",     is_required=True, default_value=True)
        self.add_argument("samtools",       is_required=True, is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=8)
        self.add_argument("mem",            is_required=True, default_value="nr_cpus * 2")

    def define_output(self, platform, split_name=None):
        # Declare merged bam output file
        bam_out = self.generate_unique_file_name(extension=".bam")
        self.add_output(platform, "bam", bam_out)

    def define_command(self, platform):
        # Obtaining the arguments
        bam_list        = self.get_arguments("bam").get_value()
        samtools        = self.get_arguments("samtools").get_value()
        nr_cpus         = self.get_arguments("nr_cpus").get_value()
        sorted_input    = self.get_arguments("bam_sorted").get_value()
        output_bam      = self.get_output("bam")

        # Generating the merging command
        if sorted_input:
            cmd = "%s merge -c -@%d %s %s" % (samtools,
                                              nr_cpus,
                                              output_bam,
                                              " ".join(bam_list))
        else:
            cmd = "%s cat -o %s %s" % (samtools,
                                       output_bam,
                                       " ".join(bam_list))
        return cmd
