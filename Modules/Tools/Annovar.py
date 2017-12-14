from Modules import Module

class Annovar(Module):

    def __init__(self, module_id):
        super(Annovar, self).__init__(module_id)

        self.input_keys     = ["vcf", "annovar", "perl", "nr_cpus", "mem"]
        self.output_keys    = ["vcf"]
        self.quick_command  = True

    def define_input(self):
        self.add_argument("vcf",                is_required=True)
        self.add_argument("annovar",            is_required=True, is_resource=True)
        self.add_argument("perl",               is_required=True, is_resource=True)
        self.add_argument("operations",         is_required=True, default_value="g,r,f,f,f,f,f,f,f,f,f,f")
        self.add_argument("protocol",           is_required=True, default_value="refGene,genomicSuperDups,exac03,snp138,esp6500_all,1000g2015aug_all,popfreq_max,cosmic70,ljb26_all,cadd13gt10,nci60,clinvar_20160302")
        self.add_argument("buildver",           is_required=True, default_value="hg19")
        self.add_argument("nastring",           is_required=True, default_value=".")
        self.add_argument("dbdir",              is_required=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=2)
        self.add_argument("mem",                is_required=True, default_value="nr_cpus * 2")

    def define_output(self, platform, split_name=None):
        # Declare VCF output filename
        vcf = self.generate_unique_file_name(split_name=split_name, extension=".hg19_multianno.vcf")
        self.add_output(platform, "vcf", vcf)

    def define_command(self, platform):
        # Get input arguments
        vcf_in      = self.get_arguments("vcf").get_value()
        annovar     = self.get_arguments("annovar").get_value()
        perl        = self.get_arguments("perl").get_value()
        operation   = self.get_arguments("operations").get_value()
        protocol    = self.get_arguments("protocol").get_value()
        nastring    = self.get_arguments("nastring").get_value()
        buildver    = self.get_arguments("buildver").get_value()
        dbdir       = self.get_arguments("dbdir").get_value()

        # Generate prefix for final VCF output file
        vcf_out = self.get_output("vcf").rsplit(".hg19_multianno.vcf", 1)[0]

        cmd = "%s %s %s %s --vcfinput --remove --buildver %s --outfile %s --protocol %s --operation %s --nastring %s !LOG3!" % (perl, annovar, vcf_in, dbdir, buildver, vcf_out, protocol, operation, nastring)
        return cmd
