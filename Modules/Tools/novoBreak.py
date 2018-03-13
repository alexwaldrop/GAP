from Modules import Module

class novoBreak(Module):
    def __init__(self, module_id):
        super(novoBreak, self).__init__(module_id)

        self.input_keys = ["bam", "is_tumor", "ref", "novoBreak", "nr_cpus", "mem"]

        self.output_keys = ["vcf"]

        self.quick_command = True

    def define_input(self):
        self.add_argument("bam",            is_required=True)
        self.add_argument("novoBreak",      is_required=True, is_resource=True)
        self.add_argument("is_tumor",       is_required=True)
        self.add_argument("ref",            is_required=True, is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=10)
        self.add_argument("mem",            is_required=True, default_value=65)

    def define_output(self, platform, split_name=None):

        # Declare the output file
        self.add_output(platform, "vcf", "novoBreak.pass.flt.vcf")

    def define_command(self, platform):

        # Get arguments to run Delly
        bam             = self.get_arguments("bam").get_value()
        is_tumor        = self.get_arguments("is_tumor").get_value()
        ref             = self.get_arguments("ref").get_value()
        nr_cpus         = self.get_arguments("nr_cpus").get_value()
        novoBreak_exe   = self.get_arguments("novoBreak").get_value()

        # Get novoBreak executing directory
        exe_dir = novoBreak_exe.rsplit("/", 1)[0]

        # Identify the tumor and the normal
        if is_tumor[0]:
            tumor = bam[0]
            normal = bam[1]
        else:
            tumor = bam[1]
            normal = bam[0]

        # Generate command
        cmd = "export PATH=$PATH:%s ; %s %s %s %s %s %s %s !LOG3!" \
              % (exe_dir, novoBreak_exe, exe_dir, ref, tumor, normal, nr_cpus, platform.get_workspace_dir())

        return cmd