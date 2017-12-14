from Modules import Module

class Cuffquant(Module):
    def __init__(self, module_id):
        super(Cuffquant, self).__init__(module_id)

        self.input_keys     = ["bam", "cuffquant", "ref", "gtf", "nr_cpus", "mem"]

        self.output_keys    = ["cuffquant_cxb"]

        self.output_prefix  = None

    def define_input(self):
        self.add_argument("bam",            is_required=True)
        self.add_argument("cuffquant",      is_required=True, is_resource=True)
        self.add_argument("ref",            is_required=True, is_resource=True)
        self.add_argument("gtf",            is_required=True, is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=8)
        self.add_argument("mem",            is_required=True, default_value="nr_cpus * 2")

    def define_output(self, platform, split_name=None):

        self.add_output(platform, "cuffquant_cxb", "abundances.cxb")

    def define_command(self, platform):

        # Get arguments to run STAR aligner
        bam         = self.get_arguments("bam").get_value()
        cuffquant   = self.get_arguments("cuffquant").get_value()
        ref         = self.get_arguments("ref").get_value()
        gtf         = self.get_arguments("gtf").get_value()
        nr_cpus     = self.get_arguments("nr_cpus").get_value()

        # Get workspace dir
        workspace_dir = platform.get_workspace_dir()

        # Design command line for Cuffquant
        cmd = "{0} {1} {2} -p {3} -b {4} -u -o {5} --no-update-check !LOG3!".format(cuffquant, gtf, bam, nr_cpus, ref, workspace_dir)

        return cmd