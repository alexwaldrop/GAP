from Modules import Module

class CombineExpressionWithMetadata(Module):
    def __init__(self, module_id):
        super(CombineExpressionWithMetadata, self).__init__(module_id)

        self.input_keys = ["expression_file", "gtf", "result_type", "combine_script", "nr_cpus", "mem"]

        self.output_keys = ["annotated_expression_file"]

        # Command should be run on main processor
        self.quick_command = True

    def define_input(self):
        self.add_argument("expression_file",    is_required=True)
        self.add_argument("gtf",                is_required=True, is_resource=True)
        self.add_argument("combine_script",     is_required=True, is_resource=True)
        self.add_argument("result_type",        is_required=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=4)
        self.add_argument("mem",                is_required=True, default_value="nr_cpus * 2")

    def define_output(self, platform, split_name=None):

        # Declare unique file name
        output_file_name = self.generate_unique_file_name(split_name=split_name,
                                                                 extension=".txt")

        self.add_output(platform, "annotated_expression_file", output_file_name)

    def define_command(self, platform):

        # Get arguments
        expression_file     = self.get_arguments("expression_file").get_value()
        gtf_file            = self.get_arguments("gtf").get_value()
        result_type         = self.get_arguments("result_type").get_value()

        #get the script that combines the expression with metadata
        combine_script = self.get_arguments("combine_script").get_value()

        #get the output file and make appropriate path for it
        output_file = self.get_output("annotated_expression_file")

        #generate command line for Rscript
        cmd = "sudo Rscript --vanilla {0} -e {1} -a {2} -t {3} -o {4} !LOG3!".format(combine_script, expression_file,
                                                                                            gtf_file, result_type,
                                                                                            output_file)

        return cmd