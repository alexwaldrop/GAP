from Modules import Module

class ViralFilter(Module):
    def __init__(self, module_id):
        super(ViralFilter, self).__init__(module_id)

        self.input_keys = ["bam", "viral_filter", "nr_cpus", "mem", "min_align_length", "min_map_quality", "only_properly_paired",
                           "max_window_length", "max_window_freq"]
        self.output_keys = ["bam"]

        # Command should be run on main processor
        self.quick_command = True

    def define_input(self):
        self.add_argument("bam",                    is_required=True)
        self.add_argument("viral_filter",           is_resource=True, is_required=True)
        self.add_argument("nr_cpus",                is_required=True, default_value=1)
        self.add_argument("mem",                    is_required=True, default_value=4)
        self.add_argument("min_align_length",       is_required=False, default_value=40)
        self.add_argument("min_map_quality",        is_required=False, default_value=30)
        self.add_argument("only_properly_paired",   is_required=False, default_value=False)
        self.add_argument("max_window_length",      is_required=False, default_value=3)
        self.add_argument("max_window_freq",        is_required=False, default_value=0.6)

    def define_output(self, platform, split_name=None):
        # Declare output bam filename
        output_bam = self.generate_unique_file_name(split_name=split_name, extension=".filtered.bam")
        self.add_output(platform, "bam", output_bam)

    def define_command(self, platform):
        # Define command for running viral filter from a platform
        bam                 = self.get_arguments("bam").get_value()
        viral_filter        = self.get_arguments("viral_filter").get_value()
        min_align_length    = self.get_arguments("min_align_length").get_value()
        min_map_quality     = self.get_arguments("min_map_quality").get_value()
        only_properly_paired = self.get_arguments("only_properly_paired").get_value()
        max_window_length   = self.get_arguments("max_window_length").get_value()
        max_window_freq     = self.get_arguments("max_window_freq").get_value()

        output_bam          = self.get_output("bam")

        # Generating filtering command
        cmd = "sudo -H pip install -U pysam; {0} {1} -v {2} -o {3} -l {4} -q {5} -w {6} -f {7}".format(
                            viral_filter, "-p" if only_properly_paired else "", bam, output_bam, min_align_length,
                            min_map_quality, max_window_length, max_window_freq)
        return cmd
