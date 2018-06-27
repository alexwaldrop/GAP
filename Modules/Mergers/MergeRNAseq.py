import os
from Modules import Merger

def generate_sample_sheet_cmd(sample_names, sample_files, outfile, in_type=None):
    # list of cmds
    cmds = list()

    #iterate through all the samples to create a sample info file for Rscript
    for index in range(len(sample_names)):
        if index == 0:
            if in_type is "cuffquant":
                cmds.append('echo -e "sample_id\\tgroup_label" > {0}'.format(outfile))
            else:
                cmds.append('echo -e "samples\\tfiles" > {0}'.format(outfile))
        if in_type is "cuffquant":
            cmds.append('echo -e "{0}\\t{1}" >> {2}'.format(sample_files[index], sample_names[index], outfile))
        else:
            cmds.append('echo -e "{0}\\t{1}" >> {2}'.format(sample_names[index], sample_files[index], outfile))
    return " ; ".join(cmds)

class AggregateRawReadCounts(Merger):
    def __init__(self, module_id, is_docker = False):
        super(AggregateRawReadCounts, self).__init__(module_id, is_docker)
        self.output_keys = ["expression_file"]

    def define_input(self):
        self.add_argument("sample_name",        is_required=True)
        self.add_argument("raw_read_counts",    is_required=True)
        self.add_argument("aggregate",          is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=2)
        self.add_argument("mem",                is_required=True, default_value="nr_cpus * 2")

    def define_output(self):

        # Declare unique file name
        output_file_name = self.generate_unique_file_name(extension=".txt")

        self.add_output("expression_file", output_file_name)

    def define_command(self):

        # Get arguments
        samples             = self.get_argument("sample_name")
        raw_read_counts     = self.get_argument("raw_read_counts")

        #get the aggregate script to run
        aggregate_script    = self.get_argument("aggregate")

        # Get current working dir
        working_dir = self.get_output_dir()

        # Generate output file name prefix for STAR
        sample_sheet_file = os.path.join(working_dir, "{0}".format("sample_info.txt"))

        #get the output file and make appropriate path for it
        output_file = self.get_output("expression_file")

        # generate command line for Rscript
        mk_sample_sheet_cmd = generate_sample_sheet_cmd(samples, raw_read_counts, sample_sheet_file)

        if not self.is_docker:
            cmd = "sudo Rscript --vanilla {0} -f {1} -o {2} !LOG3!".format(aggregate_script, sample_sheet_file, output_file)
        else:
            cmd = "Rscript --vanilla {0} -f {1} -o {2} !LOG3!".format(aggregate_script, sample_sheet_file,
                                                                           output_file)
        return "{0} ; {1}".format(mk_sample_sheet_cmd, cmd)

class AggregateRSEMResults(Merger):
    def __init__(self, module_id, is_docker = False):
        super(AggregateRSEMResults, self).__init__(module_id, is_docker)
        self.output_keys = ["isoform_expression_matrix", "isoform_expression_gene_metadata",
                            "expression_file", "gene_expression_gene_metadata"]

    def define_input(self):
        self.add_argument("sample_name",                    is_required=True)
        self.add_argument("isoforms_results",               is_required=True)
        self.add_argument("genes_results",                  is_required=True)
        self.add_argument("aggregate_rsem_results_script",  is_required=True, is_resource=True)
        self.add_argument("count_type",                     is_required=True, default_value="fpkm")
        self.add_argument("nr_cpus",                        is_required=True, default_value=8)
        self.add_argument("mem",                            is_required=True, default_value="nr_cpus * 2")

    def define_output(self):

        count_type = self.get_argument("count_type").upper()

        # Declare unique file name
        isoform_expression_file_name                = self.generate_unique_file_name(extension=".isoform_expression_matrix.{0}.txt".format(count_type))
        gene_expression_file_name                   = self.generate_unique_file_name(extension=".gene_expression_matrix.{0}.txt".format(count_type))
        isoform_expression_gene_metadata_file_name  = self.generate_unique_file_name(extension=".isoform_expression_gene_metadata.{0}.txt".format(count_type))
        gene_expression_gene_metadata_file_name     = self.generate_unique_file_name(extension=".gene_expression_gene_metadata.{0}.txt".format(count_type))

        self.add_output("isoform_expression_matrix", isoform_expression_file_name)
        self.add_output("isoform_expression_gene_metadata", isoform_expression_gene_metadata_file_name)
        self.add_output("expression_file", gene_expression_file_name)
        self.add_output("gene_expression_gene_metadata", gene_expression_gene_metadata_file_name)

    def define_command(self):

        # Get arguments
        samples                 = self.get_argument("sample_name")
        isoforms_results        = self.get_argument("isoforms_results")
        genes_results           = self.get_argument("genes_results")
        count_type              = self.get_argument("count_type")

        #transform count type to all upper case
        count_type = count_type.upper()

        #get the aggregate script to run
        aggregate_script = self.get_argument("aggregate_rsem_results_script")

        # Get current working dir
        working_dir = self.get_output_dir()

        # Generate output file name prefix for STAR
        isoforms_input_file = os.path.join(working_dir, "{0}".format("isoforms_sample_info.txt"))
        genes_input_file = os.path.join(working_dir, "{0}".format("genes_sample_info.txt"))

        #get the output file and make appropriate path for it
        isoform_expression_file_name                 = self.get_output("isoform_expression_matrix")
        gene_expression_file_name                    = self.get_output("expression_file")
        isoform_expression_gene_metadata_file_name   = self.get_output("isoform_expression_gene_metadata")
        gene_expression_gene_metadata_file_name      = self.get_output("gene_expression_gene_metadata")

        # generate command line for Rscript
        mk_sample_sheet_cmd1 = generate_sample_sheet_cmd(samples, isoforms_results, isoforms_input_file)
        mk_sample_sheet_cmd2 = generate_sample_sheet_cmd(samples, genes_results, genes_input_file)

        # generate command line for Rscript
        # possible values for count_type is TPM/FPKM/EXPECTED_COUNT
        if not self.is_docker:
            cmd = "sudo Rscript --vanilla {0} -f {1} -e {2} -m {3} -t {4} !LOG3!; " \
                  "sudo Rscript --vanilla {0} -f {5} -e {6} -m {7} -t {4} !LOG3!".format\
                (aggregate_script, isoforms_input_file, isoform_expression_file_name,
                 isoform_expression_gene_metadata_file_name, count_type,
                 genes_input_file, gene_expression_file_name, gene_expression_gene_metadata_file_name)
        else:
            cmd = "Rscript --vanilla {0} -f {1} -e {2} -m {3} -t {4} !LOG3!; " \
                  "Rscript --vanilla {0} -f {5} -e {6} -m {7} -t {4} !LOG3!".format\
                (aggregate_script, isoforms_input_file, isoform_expression_file_name,
                 isoform_expression_gene_metadata_file_name, count_type,
                 genes_input_file, gene_expression_file_name, gene_expression_gene_metadata_file_name)

        return "{0} ; {1} ; {2}".format(mk_sample_sheet_cmd1, mk_sample_sheet_cmd2, cmd)

class Cuffnorm(Merger):
    def __init__(self, module_id, is_docker = False):
        super(Cuffnorm, self).__init__(module_id, is_docker)
        self.output_keys = ["expression_file", "genes_count_table", "genes_attr_table"]

    def define_input(self):
        self.add_argument("sample_name",        is_required=True)
        self.add_argument("cuffquant_cxb",      is_required=True)
        self.add_argument("cuffnorm",           is_required=True, is_resource=True)
        self.add_argument("gtf",                is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=32)
        self.add_argument("mem",                is_required=True, default_value="nr_cpus * 6")

    def define_output(self):

        self.add_output("expression_file", "genes.fpkm_table")
        self.add_output("genes_count_table", "genes.count_table")
        self.add_output("genes_attr_table", "genes.attr_table")

    def define_command(self):

        # Get arguments
        samples             = self.get_argument("sample_name")
        cuffquant_cxbs      = self.get_argument("cuffquant_cxb")
        cuffnorm            = self.get_argument("cuffnorm")
        gtf                 = self.get_argument("gtf")
        nr_cpus             = self.get_argument("nr_cpus")

        # Get current working dir
        working_dir = self.get_output_dir()

        # Generate output file name prefix for STAR
        sample_sheet = os.path.join(working_dir, "{0}".format("cuffnorm_sample_sheet.txt"))

        # generate command line for Rscript
        mk_sample_sheet_cmd = generate_sample_sheet_cmd(samples, cuffquant_cxbs, sample_sheet, in_type="cuffquant")

        #generate command line for Rscript
        cmd = "{0} --no-update-check -v -p {1} -o {2} --use-sample-sheet {3} {4} !LOG3!".format(cuffnorm, nr_cpus, working_dir, gtf, sample_sheet)

        return "{0} ; {1}".format(mk_sample_sheet_cmd, cmd)