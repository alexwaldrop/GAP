from Modules import Module

class GVCFDistributor(Module):

    def __init__(self, module_id):
        super(GVCFDistributor, self).__init__(module_id)

        self.input_keys     = ["gvcf", "gvcf_idx", "nr_cpus", "mem"]
        self.output_keys    = ["gvcf", "gvcf_idx"]

        self.quick_command = True

    def define_input(self):
        self.add_argument("gvcf",           is_required=True)
        self.add_argument("gvcf_idx",       is_required=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=1)
        self.add_argument("mem",            is_required=True, default_value=1)

    def define_output(self, platform, split_name=None):

        gvcfs       = self.get_arguments("gvcf").get_value()
        gvcfs_idx   = self.get_arguments("gvcf_idx").get_value()

        for count, (gvcf, gvcf_idx) in enumerate(zip(gvcfs, gvcfs_idx)):
            split_name = "gvcf_%s" % count
            split_data = {
                "gvcf": gvcf,
                "gvcf_idx": gvcf_idx
            }
            self.add_output(platform, split_name, split_data, is_path=False)

    def define_command(self, platform):
        # No command needs to be run
        return None
