import os
from Modules import Module

class RSEM(Module):
    def __init__(self, module_id):
        super(RSEM, self).__init__(module_id)

        self.input_keys = ["transcriptome_mapped_bam", "rsem", "rsem_ref", "output_file_name_prefix", "nr_cpus", "mem"]

        self.output_keys = ["isoforms_results", "genes_results"]

        self.output_prefix = None

        # Command should be run on main processor
        self.quick_command = True

    def define_input(self):
        self.add_argument("transcriptome_mapped_bam",   is_required=True)
        self.add_argument("rsem",                       is_required=True, is_resource=True)
        self.add_argument("rsem_ref",                   is_required=True, is_resource=True)
        self.add_argument("output_file_name_prefix",    is_required=True, default_value="expression")
        self.add_argument("nr_cpus",                    is_required=True, default_value=8)
        self.add_argument("mem",                        is_required=True, default_value="nr_cpus * 2")

    def define_output(self, platform, split_name=None):

        # get the prefix passed as argument
        file_extension = self.get_arguments("output_file_name_prefix").get_value()

        # Declare unique file name
        output_file_name = self.generate_unique_file_name(split_name=split_name,
                                                          extension=file_extension)

        self.output_prefix = output_file_name

        self.add_output(platform, "isoforms_results", "{0}.isoforms.results".format(output_file_name))
        self.add_output(platform, "genes_results", "{0}.genes.results".format(output_file_name))

    def define_command(self, platform):

        # Get arguments
        bam         = self.get_arguments("transcriptome_mapped_bam").get_value()
        rsem        = self.get_arguments("rsem").get_value()
        rsem_ref    = self.get_arguments("rsem_ref").get_value()
        nr_cpus     = self.get_arguments("nr_cpus").get_value()

        # Get current working dir
        working_dir = platform.get_workspace_dir()

        # Generate output file name prefix for STAR
        output_file_name_prefix = os.path.join(working_dir, self.output_prefix)

        #generate command line for RSEM
        cmd = "{0} --time --bam --no-bam-output -p {1} --paired-end {2} {3} {4} !LOG3!".format(rsem, nr_cpus, bam, rsem_ref, output_file_name_prefix)

        return cmd