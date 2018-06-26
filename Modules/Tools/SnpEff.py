from Modules import Module

class SnpEff(Module):

    def __init__(self, module_id, is_docker = False):
        super(SnpEff, self).__init__(module_id, is_docker)
        self.output_keys    = ["vcf"]

    def define_input(self):
        self.add_argument("vcf",                is_required=True)
        self.add_argument("snpeff_ref",         is_required=True)
        self.add_argument("snpeff",             is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=2)
        self.add_argument("mem",                is_required=True, default_value=6)

        # Require java if not being run on docker
        if not self.is_docker:
            self.add_argument("java", is_required=True, is_resource=True)

    def define_output(self):
        # Declare VCF output filename
        vcf = self.generate_unique_file_name(extension=".vcf")
        self.add_output("vcf", vcf)

    def define_command(self):
        # Get input arguments
        vcf_in      = self.get_argument("vcf")
        snpeff      = self.get_argument("snpeff")
        snpeff_ref  = self.get_argument("snpeff_ref")
        mem         = self.get_argument("mem")

        # Get output file
        vcf_out = self.get_output("vcf")

        # Generate command with java if not running on docker
        if not self.is_docker:
            java = self.get_argument("java")
            jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem * 4 / 5, "/tmp/")
            # Generating SnpEff command
            cmd = "{0} {1} -jar {2} {3} {4} > {5} !LOG2!".format(java, jvm_options, snpeff, snpeff_ref, vcf_in, vcf_out)
        else:
            cmd = "{0} {1} {2} > {3}".format(snpeff, snpeff_ref, vcf_in, vcf_out)
        return cmd

class SnpSiftAnnotate(Module):

    def __init__(self, module_id, is_docker = False):
        super(SnpSiftAnnotate, self).__init__(module_id, is_docker)
        self.output_keys    = ["vcf"]

    def define_input(self):
        self.add_argument("vcf",                is_required=True)
        self.add_argument("snpsift",            is_required=True, is_resource=True)
        self.add_argument("dbsnp",              is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=2)
        self.add_argument("mem",                is_required=True, default_value=6)

        # Require java if not being run on docker
        if not self.is_docker:
            self.add_argument("java", is_required=True, is_resource=True)

    def define_output(self):
        # Declare VCF output filename
        vcf = self.generate_unique_file_name(extension=".vcf")
        self.add_output("vcf", vcf)

    def define_command(self):
        # Get input arguments
        vcf_in      = self.get_argument("vcf")
        snpsift     = self.get_argument("snpsift")
        dbsnp       = self.get_argument("dbsnp")
        mem         = self.get_argument("mem")

        # Get output file
        vcf_out = self.get_output("vcf")

        # Generate command with java if not running on docker
        if not self.is_docker:
            java = self.get_argument("java")
            jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem * 4 / 5, "/tmp/")
            # Generating SnpEff command
            cmd = "{0} {1} -jar {2} {3} {4} > {5} !LOG2!".format(java, jvm_options, snpsift, dbsnp, vcf_in, vcf_out)
        else:
            cmd = "{0} {1} {2} > {3}".format(snpsift, dbsnp, vcf_in, vcf_out)
        return cmd

class SnpSiftFilter(Module):

    def __init__(self, module_id, is_docker = False):
        super(SnpSiftFilter, self).__init__(module_id, is_docker)
        self.output_keys    = ["vcf"]

    def define_input(self):
        self.add_argument("vcf",                is_required=True)
        self.add_argument("snpsift",            is_required=True, is_resource=True)
        self.add_argument("filter_exp",         is_required=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=2)
        self.add_argument("mem",                is_required=True, default_value=6)

        # Require java if not being run on docker
        if not self.is_docker:
            self.add_argument("java", is_required=True, is_resource=True)

    def define_output(self):
        # Declare VCF output filename
        vcf = self.generate_unique_file_name(extension=".vcf")
        self.add_output("vcf", vcf)

    def define_command(self):
        # Get input arguments
        vcf_in              = self.get_argument("vcf")
        snpsift             = self.get_argument("snpsift")
        filter_exp          = self.get_argument("filter_exp")
        mem                 = self.get_argument("mem")

        # Get output file
        vcf_out = self.get_output("vcf")

        # Generate command with java if not running on docker
        if not self.is_docker:
            java = self.get_argument("java")
            jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem * 4 / 5, "/tmp/")
            # Generating SnpEff command
            cmd = "{0} {1} -jar {2} {3} {4} > {5} !LOG2!".format(java, jvm_options, snpsift, filter_exp, vcf_in, vcf_out)
        else:
            cmd = "{0} {1} {2} > {3}".format(snpsift, filter_exp, vcf_in, vcf_out)
        return cmd