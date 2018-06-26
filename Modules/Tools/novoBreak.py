from Modules import Module

class novoBreak(Module):
    def __init__(self, module_id, is_docker = False):
        super(novoBreak, self).__init__(module_id, is_docker)
        self.output_keys = ["vcf"]

    def define_input(self):
        self.add_argument("bam",            is_required=True)
        self.add_argument("novoBreak",      is_required=True, is_resource=True)
        self.add_argument("is_tumor",       is_required=True)
        self.add_argument("ref",            is_required=True, is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=10)
        self.add_argument("mem",            is_required=True, default_value=65)

    def define_output(self):

        # Declare the output file
        self.add_output("vcf", "novoBreak.pass.flt.vcf")

    def define_command(self):

        # Get arguments to run Delly
        bam             = self.get_argument("bam")
        is_tumor        = self.get_argument("is_tumor")
        ref             = self.get_argument("ref")
        nr_cpus         = self.get_argument("nr_cpus")
        novoBreak_exe   = self.get_argument("novoBreak")

        # get working dir
        wrk_dir = self.get_output_dir()

        # holds the NovoBreak exe if docker is not provided
        exe_dir = None

        if not self.is_docker:
            # Get novoBreak executing directory
            exe_dir = novoBreak_exe.rsplit("/", 1)[0]

        # Identify the tumor and the normal
        if is_tumor[0]:
            tumor = bam[0]
            normal = bam[1]
        else:
            tumor = bam[1]
            normal = bam[0]

        if not self.is_docker:
            # Generate command
            cmd = "export PATH=$PATH:{0} ; {1} {2} {3} {4} {5} {6} {7} !LOG3!".format(
                exe_dir, novoBreak_exe, exe_dir, ref, tumor, normal, nr_cpus, wrk_dir)
        else:
            # Generate command
            cmd = "{0} {1} {2} {3} {4} {5} {6} !LOG3!".format(novoBreak_exe, exe_dir, ref,
                                                              tumor, normal, nr_cpus, wrk_dir)

        return cmd