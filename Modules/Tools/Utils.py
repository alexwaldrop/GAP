from Modules import Module

class BGZip(Module):

    def __init__(self, module_id, is_docer = False):
        super(BGZip, self).__init__(module_id, is_docer)
        self.output_keys    = ["vcf_gz"]

    def define_input(self):
        self.add_argument("vcf",        is_required=True)                       # Input VCF file
        self.add_argument("bgzip",      is_required=True, is_resource=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=1)
        self.add_argument("mem",        is_required=True, default_value=2)

    def define_output(self):
        # Declare recoded VCF output filename
        vcf_in = self.get_argument("vcf")

        self.add_output("vcf_gz", "{0}.gz".format(vcf_in))

    def define_command(self):
        # Get input arguments
        vcf_in      = self.get_argument("vcf")
        bgzip       = self.get_argument("bgzip")
        vcf_out     = self.get_output("vcf_gz")

        # Get final normalized VCF output file path
        cmd = "{0} {1} > {2} !LOG3!".format(bgzip, vcf_in, vcf_out)
        return cmd