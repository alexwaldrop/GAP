from Modules import Splitter

class VCFSplitter(Splitter):

    def __init__(self, module_id, is_docker=False):
        super(VCFSplitter, self).__init__(module_id, is_docker)
        self.output_keys    = ["vcf"]

    def define_input(self):
        self.add_argument("vcf",                is_required=True)
        self.add_argument("snpsift",            is_required=True, is_resource=True)
        self.add_argument("vcf_chrom_list",     is_required=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=1)
        self.add_argument("mem",                is_required=True, default_value=1)

        # Conditionally require java if docker not provided
        if not self.is_docker:
            self.add_argument("java", is_required=True, is_resource=True)

    def define_output(self):
        # Get names of chromosomes in VCF file
        vcf_in = self.get_argument("vcf")
        for chrom in self.get_argument("vcf_chrom_list"):
            vcf_out = "{0}.{1}.vcf".format(vcf_in, chrom)
            self.make_split(split_id=chrom)
            self.add_output(split_id=chrom, key="vcf", value=vcf_out)

    def define_command(self):
        # Get input arguments
        vcf_in      = self.get_argument("vcf")
        snpsift     = self.get_argument("snpsift")

        # Generating SnpEff command
        if not self.is_docker:
            java = self.get_argument("java")
            return "%s -jar %s split %s !LOG3!" % (java, snpsift, vcf_in)
        else:
            return "%s split %s !LOG3!" % (snpsift, vcf_in)