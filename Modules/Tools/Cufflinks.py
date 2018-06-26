import os
from Modules import Module

class Cuffquant(Module):
    def __init__(self, module_id, is_docker = False):
        super(Cuffquant, self).__init__(module_id, is_docker)
        self.output_keys    = ["cuffquant_cxb"]

    def define_input(self):
        self.add_argument("bam",            is_required=True)
        self.add_argument("cuffquant",      is_required=True, is_resource=True)
        self.add_argument("ref",            is_required=True, is_resource=True)
        self.add_argument("gtf",            is_required=True, is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=8)
        self.add_argument("mem",            is_required=True, default_value="nr_cpus * 2")

    def define_output(self):

        output_file = os.path.join(self.get_output_dir(), "abundances.cxb")
        self.add_output("cuffquant_cxb", output_file)

    def define_command(self):

        # Get arguments to run STAR aligner
        bam         = self.get_argument("bam")
        cuffquant   = self.get_argument("cuffquant")
        ref         = self.get_argument("ref")
        gtf         = self.get_argument("gtf")
        nr_cpus     = self.get_argument("nr_cpus")

        # Get workspace dir
        workspace_dir = self.get_output_dir()

        # Design command line for Cuffquant
        cmd = "{0} {1} {2} -p {3} -b {4} -u -o {5} --no-update-check !LOG3!".format(cuffquant, gtf, bam, nr_cpus, ref, workspace_dir)

        return cmd