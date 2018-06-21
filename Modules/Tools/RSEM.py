import os
from Modules import Module

class RSEM(Module):
    def __init__(self, module_id, is_docker = False):
        super(RSEM, self).__init__(module_id, is_docker)
        self.output_keys = ["isoforms_results", "genes_results"]

    def define_input(self):
        self.add_argument("transcriptome_mapped_bam",   is_required=True)
        self.add_argument("rsem",                       is_required=True, is_resource=True)
        self.add_argument("rsem_ref",                   is_required=True, is_resource=True)
        self.add_argument("paired_end",                 is_required=True, default_value=True)
        self.add_argument("nr_cpus",                    is_required=True, default_value=8)
        self.add_argument("mem",                        is_required=True, default_value="nr_cpus * 2")

    def define_output(self):

        # Declare unique file name
        output_file_name = self.generate_unique_file_name(extension=".txt").split(".")[0]

        self.add_output("isoforms_results", "{0}.isoforms.results".format(output_file_name))
        self.add_output("genes_results", "{0}.genes.results".format(output_file_name))

    def define_command(self):

        # Get arguments
        bam         = self.get_argument("transcriptome_mapped_bam")
        rsem        = self.get_argument("rsem")
        rsem_ref    = self.get_argument("rsem_ref")
        paired_end  = self.get_argument("paired_end")
        nr_cpus     = self.get_argument("nr_cpus")

        # get the path out of the GAP file object
        output_file_path = self.get_output("genes_results").get_path()

        # Generate output file name prefix for RSEM
        output_file_name_prefix = output_file_path.split(".")[0]

        #generate command line for RSEM according to input read type
        if paired_end:
            cmd = "{0} --time --bam --no-bam-output -p {1} --paired-end {2} {3} {4} !LOG3!".format(rsem, nr_cpus, bam,
                                                                                                   rsem_ref,output_file_name_prefix)
        else:
            cmd = "{0} --time --bam --no-bam-output -p {1} {2} {3} {4} !LOG3!".format(rsem, nr_cpus, bam,
                                                                                      rsem_ref, output_file_name_prefix)

        return cmd