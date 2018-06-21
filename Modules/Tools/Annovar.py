from Modules import Module

class Annovar(Module):

    def __init__(self, module_id, is_docker = False):
        super(Annovar, self).__init__(module_id, is_docker)
        self.output_keys    = ["vcf"]

    def define_input(self):
        self.add_argument("vcf",                is_required=True)
        self.add_argument("annovar",            is_required=True, is_resource=True)
        self.add_argument("perl",               is_required=True, is_resource=True)
        self.add_argument("operations",         is_required=True, default_value="g,r,f,f,f,f,f,f,f,f,f,f,f,f")
        self.add_argument("protocol",           is_required=True, default_value="refGene,genomicSuperDups,exac03,gnomad_exome,gnomad_genome,snp138,esp6500_all,1000g2015aug_all,popfreq_max,cosmic70,ljb26_all,cadd13gt10,nci60,clinvar_20160302")
        self.add_argument("buildver",           is_required=True, default_value="hg19")
        self.add_argument("nastring",           is_required=True, default_value=".")
        self.add_argument("dbdir",              is_required=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=6)
        self.add_argument("mem",                is_required=True, default_value="nr_cpus * 6.5")

    def define_output(self):
        # Declare VCF output filename
        vcf = self.generate_unique_file_name(extension=".hg19_multianno.vcf")
        self.add_output("vcf", vcf)

    def define_command(self):
        # Get input arguments
        vcf_in      = self.get_argument("vcf")
        annovar     = self.get_argument("annovar")
        perl        = self.get_argument("perl")
        operation   = self.get_argument("operations")
        protocol    = self.get_argument("protocol")
        nastring    = self.get_argument("nastring")
        buildver    = self.get_argument("buildver")
        dbdir       = self.get_argument("dbdir")

        # Generate prefix for final VCF output file
        vcf_out = self.get_output("vcf").rsplit(".hg19_multianno.vcf", 1)[0]

        cmd = "{0} {1} {2} {3} --vcfinput --remove --buildver {4} --outfile {5} --protocol {6} --operation {7} --nastring {8} !LOG3!".format\
                (perl, annovar, vcf_in, dbdir, buildver, vcf_out, protocol, operation, nastring)
        return cmd
