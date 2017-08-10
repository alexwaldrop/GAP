from Module import Module

class SamtoolsDepthMerge(Module):
    def __init__(self, module_id):
        super(SamtoolsDepthMerge, self).__init__(module_id)

        self.input_keys     = ["samtools_depth"]
        self.output_keys    = ["samtools_depth"]

    def define_input(self):
        self.add_argument("samtools_depth", required=True)

    def define_output(self, platform, split_name=None):
        # Declare merged samtools depth output filename
        merged_out = self.generate_unique_file_name(extension=".samtoolsdepth.out")
        self.add_output(platform, "samtools_depth", merged_out)

    def get_command(self, platform):
        samtools_depth_in   = self.get_arguments("samtools_depth").get_value()
        merged_out          = self.get_output("samtools_depth")

        # Generating command for concatenating multiple files together using unix Cat command
        cmd = "cat %s > %s !LOG2!" % (" ".join(samtools_depth_in),merged_out)
        return cmd