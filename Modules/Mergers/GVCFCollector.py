from Modules import Module

class GVCFCollector(Module):

    def __init__(self, module_id):
        super(GVCFCollector, self).__init__(module_id)

        self.input_keys     = ["gvcf", "gvcf_idx", "nr_cpus", "mem"]
        self.output_keys    = ["gvcf", "gvcf_idx"]

        self.quick_command = True

    def define_input(self):
        self.add_argument("gvcf",           is_required=True)
        self.add_argument("gvcf_idx",       is_required=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=1)
        self.add_argument("mem",            is_required=True, default_value=1)

    def define_output(self, platform, split_name=None):

        # Collect all the gvcfs
        gvcfs       = self.get_arguments("gvcf").get_value()
        self.add_output(platform, "gvcf", gvcfs, is_path=False)

        # Collect all the gvcf_idx
        gvcfs_idx   = self.get_arguments("gvcf_idx").get_value()
        self.add_output(platform, "gvcf_idx", gvcfs_idx, is_path=False)

    def define_command(self, platform):
        # No command needs to be run
        return None
