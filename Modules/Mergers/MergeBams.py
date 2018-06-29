from Modules import Merger

class MergeBams(Merger):

    def __init__(self, module_id, is_docker=False):
        super(MergeBams, self).__init__(module_id, is_docker)
        self.output_keys  = ["bam", "bam_idx"]

    def define_input(self):
        self.add_argument("bam",            is_required=True)
        self.add_argument("bam_sorted",     is_required=True, default_value=True)
        self.add_argument("samtools",       is_required=True, is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=8)
        self.add_argument("mem",            is_required=True, default_value="nr_cpus * 2")

    def define_output(self):
        # Declare merged bam output file
        bam_out = self.generate_unique_file_name(extension=".bam")
        bam_idx = "%s.bai" % bam_out
        self.add_output("bam",      bam_out)
        self.add_output("bam_idx",  bam_idx)

    def define_command(self):
        # Obtaining the arguments
        bam_list        = self.get_argument("bam")
        samtools        = self.get_argument("samtools")
        nr_cpus         = self.get_argument("nr_cpus")
        sorted_input    = self.get_argument("bam_sorted")
        output_bam      = self.get_output("bam")
        output_bam_idx  = self.get_output("bam_idx")

        # Generating the merging command
        if sorted_input:
            merge_cmd = "%s merge -f -c -@%d %s %s" % (samtools,
                                                       nr_cpus,
                                                       output_bam,
                                                       " ".join(bam_list))
        else:
            merge_cmd = "%s cat -o %s %s" % (samtools,
                                             output_bam,
                                             " ".join(bam_list))

        # Generate command to make index
        index_cmd = "%s %s %s" % (samtools, output_bam, output_bam_idx)

        # Return command for
        return "%s !LOG2! && %s !LOG2!" % (merge_cmd, index_cmd)

