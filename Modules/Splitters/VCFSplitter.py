from Modules import Module

class VCFSplitter(Module):

    def __init__(self, module_id):
        super(VCFSplitter, self).__init__(module_id)

        self.input_keys     = ["vcf", "snpsift", "java", "nr_cpus", "mem"]
        self.output_keys    = ["vcf"]
        self.quick_command  = True

    def define_input(self):
        self.add_argument("vcf",                is_required=True)
        self.add_argument("snpsift",            is_required=True, is_resource=True)
        self.add_argument("java",               is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=1)
        self.add_argument("mem",                is_required=True, default_value=1)

    def define_output(self, platform, split_name=None):

        # Get names of chromosomes in VCF file
        vcf_in          = self.get_arguments("vcf").get_value()
        basename = vcf_in.split(".vcf")[0]
        get_names_cmd   = 'cat %s | grep -v "#" | cut -f1 | sort | uniq' % vcf_in
        out, err        = platform.run_quick_command("get_vcf_chroms", get_names_cmd)

        for line in out.split("\n"):
            # Skip empty lines
            if len(line) > 0:
                # Create split for next chromosome
                chrom   = line
                vcf_out = "%s.%s.vcf" % (basename, chrom)
                self.add_output(platform, chrom, {"vcf":vcf_out}, is_path=False)

    def define_command(self, platform):
        # Get input arguments
        vcf_in      = self.get_arguments("vcf").get_value()
        snpsift     = self.get_arguments("snpsift").get_value()
        java        = self.get_arguments("java").get_value()

        # Generating SnpEff command
        cmd = "%s -jar %s split %s !LOG3!" % (java, snpsift, vcf_in)
        return cmd