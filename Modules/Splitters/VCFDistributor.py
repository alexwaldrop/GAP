from Modules import Module

class VCFDistributor(Module):

    def __init__(self, module_id):
        super(VCFDistributor, self).__init__(module_id)

        self.input_keys     = ["vcf", "nr_cpus", "mem"]
        self.output_keys    = ["vcf"]
        self.quick_command = True

    def define_input(self):
        self.add_argument("vcf",        is_required=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=1)
        self.add_argument("mem",        is_required=True, default_value=1)

    def define_output(self, platform, split_name=None):
        vcfs       = self.get_arguments("vcf").get_value()
        for count, vcf in enumerate(vcfs):
            split_name = "vcf_%s" % count
            split_data = {"vcf": vcf}
            self.add_output(platform, split_name, split_data, is_path=False)

    def define_command(self, platform):
        # No command needs to be run
        return None
