from Modules import Module

class ParseSamtoolsDepth(Module):

    def __init__(self, module_id):
        super(ParseSamtoolsDepth, self).__init__(module_id)

        self.input_keys     = ["samtools_depth", "qc_parser", "note", "nr_cpus", "mem"]
        self.output_keys    = ["qc_report"]

        self.quick_command  = True

    def define_input(self):
        self.add_argument("samtools_depth",     is_required=True)
        self.add_argument("sample_name",        is_required=True)
        self.add_argument("note",               is_required=False, default_value=None)
        self.add_argument("qc_parser",          is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=1)
        self.add_argument("mem",                is_required=True, default_value=12)
        self.add_argument("depth_cutoffs",      is_required=True, default_value=[1,5,10,15,25,50,100])

    def define_output(self, platform, split_name=None):
        # Declare output summary filename
        summary_file = self.generate_unique_file_name(split_name=split_name, extension=".depth.qc_report.json")
        self.add_output(platform, "qc_report", summary_file)

    def define_command(self, platform):

        # Get options from kwargs
        input_file      = self.get_arguments("samtools_depth").get_value()
        qc_parser       = self.get_arguments("qc_parser").get_value()
        sample_name     = self.get_arguments("sample_name").get_value()
        cutoffs         = self.get_arguments("depth_cutoffs").get_value()
        parser_note     = self.get_arguments("note").get_value()
        qc_report       = self.get_output("qc_report")

        # Generating command to parse samtools depth output
        cmd = "%s SamtoolsDepth -i %s -s %s " % (qc_parser, input_file, sample_name)

        # Add options for coverage depth cutoffs to report
        for cutoff in cutoffs:
            cutoff = int(cutoff)
            cmd += " --ct %d" % cutoff

        # Add parser note if necessary
        if parser_note is not None:
            cmd += " -n \"%s\"" % parser_note

        # Write output to summary file
        cmd += " > %s !LOG2!" % qc_report
        return cmd
