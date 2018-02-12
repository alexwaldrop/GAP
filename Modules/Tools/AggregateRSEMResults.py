import os
from Modules import Module

class AggregateRSEMResults(Module):
    def __init__(self, module_id):
        super(AggregateRSEMResults, self).__init__(module_id)

        self.input_keys = ["sample_name", "isoforms_results", "genes_results", "count_type",
                           "aggregate_rsem_results_script", "nr_cpus", "mem"]

        self.output_keys = ["isoform_expression_matrix", "isoform_expression_gene_metadata",
                            "expression_file", "gene_expression_gene_metadata"]

        # Command should be run on main processor
        self.quick_command = True

    def define_input(self):
        self.add_argument("sample_name",                    is_required=True)
        self.add_argument("isoforms_results",               is_required=True)
        self.add_argument("genes_results",                  is_required=True)
        self.add_argument("aggregate_rsem_results_script",  is_required=True, is_resource=True)
        self.add_argument("count_type",                     is_required=True, default_value="fpkm")
        self.add_argument("nr_cpus",                        is_required=True, default_value=8)
        self.add_argument("mem",                            is_required=True, default_value="nr_cpus * 2")

    def define_output(self, platform, split_name=None):

        count_type = self.get_arguments("count_type").get_value().upper()

        # Declare unique file name
        isoform_expression_file_name                = self.generate_unique_file_name(split_name=split_name,
                                                                                     extension=".isoform_expression_matrix.{0}.txt".format(count_type))
        gene_expression_file_name                   = self.generate_unique_file_name(split_name=split_name,
                                                                                     extension=".gene_expression_matrix.{0}.txt".format(count_type))
        isoform_expression_gene_metadata_file_name  = self.generate_unique_file_name(split_name=split_name,
                                                                                     extension=".isoform_expression_gene_metadata.{0}.txt".format(count_type))
        gene_expression_gene_metadata_file_name     = self.generate_unique_file_name(split_name=split_name,
                                                                                     extension=".gene_expression_gene_metadata.{0}.txt".format(count_type))

        self.add_output(platform, "isoform_expression_matrix", isoform_expression_file_name)
        self.add_output(platform, "isoform_expression_gene_metadata", isoform_expression_gene_metadata_file_name)
        self.add_output(platform, "expression_file", gene_expression_file_name)
        self.add_output(platform, "gene_expression_gene_metadata", gene_expression_gene_metadata_file_name)

    def define_command(self, platform):

        # Get arguments
        samples                 = self.get_arguments("sample_name").get_value()
        isoforms_results        = self.get_arguments("isoforms_results").get_value()
        genes_results           = self.get_arguments("genes_results").get_value()
        count_type              = self.get_arguments("count_type").get_value()

        #transform count type to all upper case
        count_type = count_type.upper()

        #get the aggregate script to run
        aggregate_script = self.get_arguments("aggregate_rsem_results_script").get_value()

        # Get current working dir
        working_dir = platform.get_workspace_dir()

        # Generate output file name prefix for STAR
        isoforms_input_file = os.path.join(working_dir, "{0}".format("isoforms_sample_info.txt"))
        genes_input_file = os.path.join(working_dir, "{0}".format("genes_sample_info.txt"))

        #get the output file and make appropriate path for it
        isoform_expression_file_name                 = self.get_output("isoform_expression_matrix")
        gene_expression_file_name                    = self.get_output("expression_file")
        isoform_expression_gene_metadata_file_name   = self.get_output("isoform_expression_gene_metadata")
        gene_expression_gene_metadata_file_name      = self.get_output("gene_expression_gene_metadata")

        #iterate through all the samples to create a sample info file for Rscript
        for index in range(len(samples)):
            if index == 0:
                cmd = 'echo -e "samples\\tfiles" > {0}'.format(isoforms_input_file)
                platform.run_quick_command("make_sample_info_file", cmd)
                cmd = 'echo -e "samples\\tfiles" > {0}'.format(genes_input_file)
                platform.run_quick_command("make_sample_info_file", cmd)
            cmd = 'echo -e "{0}\\t{1}" >> {2}'.format(samples[index], isoforms_results[index], isoforms_input_file)
            platform.run_quick_command("make_sample_info_file", cmd)
            cmd = 'echo -e "{0}\\t{1}" >> {2}'.format(samples[index], genes_results[index], genes_input_file)
            platform.run_quick_command("make_sample_info_file", cmd)

        #generate command line for Rscript
        cmd = "sudo Rscript --vanilla {0} -f {1} -e {2} -m {3} -t {4} !LOG3!; " \
              "sudo Rscript --vanilla {0} -f {5} -e {6} -m {7} -t {4} !LOG3!" \
                .format(aggregate_script, isoforms_input_file, isoform_expression_file_name,
                        isoform_expression_gene_metadata_file_name, count_type,
                        genes_input_file, gene_expression_file_name, gene_expression_gene_metadata_file_name)
        return cmd