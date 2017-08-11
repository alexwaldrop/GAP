from Modules import Module

class SummarizeSamtoolsDepth(Module):

    def __init__(self, module_id):
        super(SummarizeSamtoolsDepth, self).__init__(module_id)

        self.input_keys     = ["samtools_depth", "qc_parser", "nr_cpus", "mem"]
        self.output_keys    = ["summary_file"]

    def define_input(self):
        self.add_argument("samtools_depth",     is_required=True)
        self.add_argument("qc_parser",          is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default=1)
        self.add_argument("mem",                is_required=True, default=12)
        self.add_argument("depth_cutoffs",      is_required=True, default=[1,5,10,15,25,50,100])

    def define_output(self, platform, split_name=None):
        # Declare output summary filename
        summary_file = self.generate_unique_file_name(split_name=split_name, extension=".depth.summary.txt")
        self.add_output(platform, "summary_file", summary_file)

    def get_command(self, platform):

        # Get options from kwargs
        input           = self.get_arguments("samtools_depth").get_value()
        qc_parser       = self.get_arguments("qc_parser").get_value()
        cutoffs         = self.get_arguments("depth_cutoffs").get_value()
        summary_file    = self.get_output("summary_file")

        # Generating command to parse samtools depth output
        cmd = "%s coverage -i %s" % (qc_parser, input)

        # Add options for coverage depth cutoffs to report
        for cutoff in cutoffs:
            cutoff = int(cutoff)
            cmd += " --ct %d" % cutoff

        # Write output to summary file
        cmd += " > %s !LOG2!" % summary_file
        return cmd
