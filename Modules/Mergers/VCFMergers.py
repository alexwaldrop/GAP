from Modules import Merger

class VCFMerger(Merger):

    def __init__(self, module_id, is_docker=False):
        super(VCFMerger, self).__init__(module_id, is_docker)
        self.output_keys    = ["vcf"]

    def define_input(self):
        self.add_argument("vcf",                is_required=True)
        self.add_argument("snpsift",            is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value="MAX")
        self.add_argument("mem",                is_required=True, default_value="MAX")

        # Conditionally require java if not docker
        if not self.is_docker:
            self.add_argument("java", is_required=True, is_resource=True)

    def define_output(self):
        # Declare name of merged VCF output file
        vcf_out = self.generate_unique_file_name(extension=".vcf")
        self.add_output("vcf", vcf_out)

    def define_command(self):
        # Get input arguments
        vcf_list    = self.get_argument("vcf")
        snpsift     = self.get_argument("snpsift")
        vcf_out     = self.get_output("vcf")

        # Generating JVM options
        if not self.is_docker:
            java = self.get_argument("java")
            mem = self.get_argument("mem")
            jvm_options = "-Xmx%dG -Djava.io.tmpdir=/tmp/" % (mem * 9 / 10)
            snpsift_cmd = "{0} {1} -jar {2}".format(java, jvm_options, snpsift)
        else:
            snpsift_cmd = str(snpsift)

        # Generating SnpEff command
        return "{0} sort {1} > {2} !LOG2!".format(snpsift_cmd, " ".join(vcf_list), vcf_out)


class BGZipVCFMerger(Merger):

    def __init__(self, module_id):
        super(BGZipVCFMerger, self).__init__(module_id)
        self.output_keys    = ["vcf"]

    def define_input(self):
        self.add_argument("vcf_gz",     is_required=True)
        self.add_argument("vcf_csi",    is_required=True)
        self.add_argument("bcftools",   is_required=True,   is_resource=True)
        self.add_argument("nr_cpus",    is_required=True,   default_value=4)
        self.add_argument("mem",        is_required=True,   default_value="nr_cpus*4")

    def define_output(self):
        # Declare recoded VCF output filename
        vcf_out = self.generate_unique_file_name(extension=".vcf")
        self.add_output("vcf", vcf_out)

    def define_command(self):
        # Get input arguments
        vcf_gz      = self.get_argument("vcf_gz")
        bcftools    = self.get_argument("bcftools")
        vcf_out     = self.get_output("vcf")
        # Get final normalized VCF output file path
        cmd = "%s merge -F x %s > %s !LOG2!" % (bcftools, " ".join(vcf_gz), vcf_out)
        return cmd


class RecodedVCFMerger(Merger):

    def __init__(self, module_id, is_docker=False):
        super(RecodedVCFMerger, self).__init__(module_id, is_docker)
        self.output_keys    = ["recoded_vcf"]

    def define_input(self):
        self.add_argument("recoded_vcf",        is_required=True)
        self.add_argument("cat_recoded_vcf",    is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True,   default_value=8)
        self.add_argument("mem",                is_required=True,   default_value=16)

    def define_output(self):
        # Declare merged samtools depth output filename
        recoded_vcf_out = self.generate_unique_file_name(extension=".recoded.vcf.txt")
        self.add_output("recoded_vcf", recoded_vcf_out)

    def define_command(self):
        cat_recode_vcf  = self.get_argument("cat_recoded_vcf")
        recode_vcf_in   = self.get_argument("recoded_vcf")
        recode_vcf_out  = self.get_output("recoded_vcf")

        # Generate cat recoded VCF command

        # Install pyvcf prior to runtime if not running in docker
        if not self.is_docker:
            return "sudo pip install -U pyvcf ; python %s -i %s -vvv --output %s !LOG2!" % (cat_recode_vcf, " ".join(recode_vcf_in), recode_vcf_out)

        # Otherwise just let it rip
        return "%s -i %s -vvv --output %s !LOG2!" % (cat_recode_vcf, " ".join(recode_vcf_in), recode_vcf_out)


class VCFSummaryMerger(Merger):
    def __init__(self, module_id, is_docker=False):
        super(VCFSummaryMerger, self).__init__(module_id, is_docker)
        self.output_keys    = ["vcf_summary"]

    def define_input(self):
        self.add_argument("vcf_summary",        is_required=True)
        self.add_argument("cat_vcf_summary",    is_required=True,   is_resource=True)
        self.add_argument("nr_cpus",            is_required=True,   default_value=8)
        self.add_argument("mem",                is_required=True,   default_value=16)

    def define_output(self):
        # Declare merged samtools depth output filename
        vcf_summary_out = self.generate_unique_file_name(extension=".summary.txt")
        self.add_output("vcf_summary", vcf_summary_out)

    def define_command(self):
        cat_vcf_summary  = self.get_argument("cat_vcf_summary")
        vcf_summary_in   = self.get_argument("vcf_summary")
        vcf_summary_out  = self.get_output("vcf_summary")

        # Generate command to merge VCF summaries

        # Install pyVCF before running if not running in Docker
        if not self.is_docker:
            return "sudo pip install -U pyvcf ; python %s -i %s -vvv > %s !LOG2!" % (cat_vcf_summary, " ".join(vcf_summary_in), vcf_summary_out)

        return "%s -i %s -vvv > %s !LOG2!" % (cat_vcf_summary, " ".join(vcf_summary_in), vcf_summary_out)