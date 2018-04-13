import os
from Modules import Module

class Cuffnorm(Module):
    def __init__(self, module_id):
        super(Cuffnorm, self).__init__(module_id)

        self.input_keys = ["sample_name", "cuffquant_cxb", "cuffnorm", "gtf", "nr_cpus", "mem"]

        self.output_keys = ["expression_file", "genes_count_table", "genes_attr_table"]

        # Command should be run on main processor
        self.quick_command = False

    def define_input(self):
        self.add_argument("sample_name",        is_required=True)
        self.add_argument("cuffquant_cxb",      is_required=True)
        self.add_argument("cuffnorm",           is_required=True, is_resource=True)
        self.add_argument("gtf",                is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=32)
        self.add_argument("mem",                is_required=True, default_value="nr_cpus * 6")

    def define_output(self, platform, split_name=None):

        self.add_output(platform, "expression_file", "genes.fpkm_table")
        self.add_output(platform, "genes_count_table", "genes.count_table")
        self.add_output(platform, "genes_attr_table", "genes.attr_table")

    def define_command(self, platform):

        # Get arguments
        samples             = self.get_arguments("sample_name").get_value()
        cuffquant_cxbs      = self.get_arguments("cuffquant_cxb").get_value()
        cuffnorm            = self.get_arguments("cuffnorm").get_value()
        gtf                 = self.get_arguments("gtf").get_value()
        nr_cpus             = self.get_arguments("nr_cpus").get_value()

        # Get current working dir
        working_dir = platform.get_workspace_dir()

        # Generate output file name prefix for STAR
        sample_sheet = os.path.join(working_dir, "{0}".format("cuffnorm_sample_sheet.txt"))

        #iterate through all the samples to create a sample info file for Rscript
        for index in range(len(samples)):
            if index == 0:
                cmd = 'echo -e "sample_id\\tgroup_label" > {0}'.format(sample_sheet)
                platform.run_quick_command("make_cuffnorm_sample_sheet", cmd)
            cmd = 'echo -e "{0}\\t{1}" >> {2}'.format(cuffquant_cxbs[index], samples[index], sample_sheet)
            platform.run_quick_command("make_cuffnorm_sample_sheet", cmd)

        #generate command line for Rscript
        cmd = "{0} --no-update-check -v -p {1} -o {2} --use-sample-sheet {3} {4} !LOG3!".format(cuffnorm, nr_cpus, working_dir, gtf, sample_sheet)

        return cmd