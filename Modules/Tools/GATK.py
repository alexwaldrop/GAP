import logging

from Modules import Module


class _GATKBase(Module):

    def __init__(self, module_id, is_docker=False):
        super(_GATKBase, self).__init__(module_id, is_docker)

    def define_base_args(self):
        self.add_argument("java",           is_required=True, is_resource=True)
        self.add_argument("gatk",           is_required=True, is_resource=True)
        self.add_argument("gatk_version",   is_required=True)
        self.add_argument("location")
        self.add_argument("excluded_location")

    def get_gatk_command(self):
        # Get input arguments
        gatk    = self.get_argument("gatk")
        mem     = self.get_argument("mem")
        java = self.get_argument("java")
        jvm_options = "-Xmx{0}G -Djava.io.tmpdir={1}".format(mem * 4 / 5, "/tmp/")

        # Determine numeric version of GATK
        gatk_version = self.get_argument("gatk_version")
        gatk_version = str(gatk_version).lower().replace("gatk","")
        gatk_version = gatk_version.strip()
        gatk_version = int(gatk_version.split(".")[0])

        if gatk_version < 4:
            return "{0} {1} -jar {2} -T".format(java, jvm_options, gatk)

        # Generate base command with endpoint provided by docker
        else:
            return "{0} {1} -jar {2}".format(java, jvm_options, gatk)


class HaplotypeCaller(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(HaplotypeCaller, self).__init__(module_id, is_docker)
        self.output_keys = ["gvcf", "gvcf_idx"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("bam",                is_required=True)
        self.add_argument("bam_idx",            is_required=True)
        self.add_argument("BQSR_report",        is_required=True)
        self.add_argument("ref",                is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=8)
        self.add_argument("mem",                is_required=True, default_value=48)

    def define_output(self):
        # Declare GVCF output filename
        gvcf = self.generate_unique_file_name(extension=".g.vcf")
        self.add_output("gvcf", gvcf)
        # Declare GVCF index output filename
        gvcf_idx = self.generate_unique_file_name(extension=".g.vcf.idx")
        self.add_output("gvcf_idx", gvcf_idx)

    def define_command(self):
        # Get input arguments
        bam     = self.get_argument("bam")
        BQSR    = self.get_argument("BQSR_report")
        ref     = self.get_argument("ref")
        L       = self.get_argument("location")
        XL      = self.get_argument("excluded_location")
        gvcf    = self.get_output("gvcf")
        gatk_cmd = self.get_gatk_command()

        # Generating the haplotype caller options
        opts = list()
        opts.append("-I %s" % bam)
        opts.append("-o %s" % gvcf)
        opts.append("-R %s" % ref)
        opts.append("-ERC GVCF")
        if BQSR is not None:
            opts.append("-BQSR %s" % BQSR)

        # Limit the locations to be processes
        if L is not None:
            if isinstance(L, list):
                for included in L:
                    if included != "unmapped":
                        opts.append("-L \"%s\"" % included)
            else:
                opts.append("-L \"%s\"" % L)
        if XL is not None:
            if isinstance(XL, list):
                for excluded in XL:
                    opts.append("-XL \"%s\"" % excluded)
            else:
                opts.append("-XL \"%s\"" % XL)

        # Generating command for HaplotypeCaller
        return "%s HaplotypeCaller %s !LOG3!" % (gatk_cmd, " ".join(opts))

class PrintReads(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(PrintReads, self).__init__(module_id, is_docker)
        self.output_keys            = ["bam"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("bam",                is_required=True)
        self.add_argument("bam_idx",            is_required=True)
        self.add_argument("BQSR_report",        is_required=True)
        self.add_argument("ref",                is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=2)
        self.add_argument("mem",                is_required=True, default_value="nr_cpus * 2.5")

    def define_output(self):
        # Declare bam output filename
        bam = self.generate_unique_file_name(extension=".recalibrated.bam")
        self.add_output("bam", bam)

    def define_command(self):
        # Obtaining the arguments
        bam     = self.get_argument("bam")
        BQSR    = self.get_argument("BQSR_report")
        ref     = self.get_argument("ref")
        L       = self.get_argument("location")
        XL      = self.get_argument("excluded_location")
        nr_cpus = self.get_argument("nr_cpus")
        output_bam = self.get_output("bam")
        gatk_cmd = self.get_gatk_command()

        # Generating the PrintReads caller options
        opts = list()
        opts.append("-I %s" % bam)
        opts.append("-o %s" % output_bam)
        opts.append("-nct %d" % nr_cpus)
        opts.append("-R %s" % ref)
        opts.append("-BQSR %s" % BQSR)

        # Limit the locations to be processed
        if L is not None:
            if isinstance(L, list):
                for included in L:
                    opts.append("-L \"%s\"" % included)
            else:
                opts.append("-L \"%s\"" % L)
        if XL is not None:
            if isinstance(XL, list):
                for excluded in XL:
                    opts.append("-XL \"%s\"" % excluded)
            else:
                opts.append("-XL \"%s\"" % XL)

        # Generating command for GATK PrintReads
        return "%s PrintReads %s !LOG3!" % (gatk_cmd, " ".join(opts))

class BaseRecalibrator(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(BaseRecalibrator, self).__init__(module_id, is_docker)
        self.output_keys    = ["BQSR_report"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("bam",                is_required=True)
        self.add_argument("bam_idx",            is_required=True)
        self.add_argument("chrom_size_list",    is_required=False)
        self.add_argument("ref",                is_required=True, is_resource=True)
        self.add_argument("dbsnp",              is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value="MAX")
        self.add_argument("mem",                is_required=True, default_value="nr_cpus * 2")
        self.add_argument("max_nr_reads",       is_required=True, default_value=2.5*10**7)

    def define_output(self):
        # Declare BQSR report file
        bqsr_report = self.generate_unique_file_name(extension=".grp")
        self.add_output("BQSR_report", bqsr_report)

    def define_command(self):
        # Get arguments needed to generate GATK BQSR command
        bam             = self.get_argument("bam")
        chrom_size_list = self.get_argument("chrom_size_list")
        ref             = self.get_argument("ref")
        dbsnp           = self.get_argument("dbsnp")
        nr_cpus         = self.get_argument("nr_cpus")
        max_nr_reads    = self.get_argument("max_nr_reads")
        bqsr_report     = self.get_output("BQSR_report")

        gatk_cmd        = self.get_gatk_command()

        # Convert max_nr_reads to integer if necessary
        max_nr_reads    = eval(max_nr_reads) if isinstance(max_nr_reads, basestring) else max_nr_reads

        # Generating the base recalibration options
        opts = list()
        opts.append("-I %s" % bam)
        opts.append("-o %s" % bqsr_report)
        opts.append("-nct %d" % nr_cpus)
        opts.append("-R %s" % ref)
        opts.append("-knownSites %s" % dbsnp)
        opts.append("-cov ReadGroupCovariate")
        opts.append("-cov QualityScoreCovariate")
        opts.append("-cov CycleCovariate")
        opts.append("-cov ContextCovariate")

        # Limit the number of reads processed
        try:
            if chrom_size_list is not None:
                logging.info("Determining chromosomes to include for BQSR...")
                chrom_list = BaseRecalibrator.__get_chrom_locations(chrom_size_list, max_nr_reads)
                # Add the minimum amount of chromosomes to exceed the max_read_nr
                if chrom_list is not None:
                    for chrom in chrom_list:
                        opts.append("-L \"%s\"" % chrom)
        except:
            logging.error("Unable to determine the number of chromosomes for BQSR!")
            raise

        # Generating command for base recalibration
        return "{0} BaseRecalibrator {1} !LOG3!".format(gatk_cmd, " ".join(opts))

    @staticmethod
    def __get_chrom_locations(chrom_size_list, max_nr_reads):
        # Obtaining the chromosome alignment information
        # Analysing the output of idxstats to identify which chromosome location is needed we need
        chrom_list = list()
        total = 0
        for chrom in chrom_size_list:
            chrom_name = chrom[0]
            chrom_size = chrom[1]
            if chrom_name != "*":
                chrom_list.append(chrom_name)
                total += int(chrom_size)
            # If we reached more than maximum number reads, then return the current available list
            if total >= int(max_nr_reads):
                return chrom_list
        # If here, then process the entire file
        return None

class IndexVCF(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(IndexVCF, self).__init__(module_id, is_docker)
        self.output_keys  = ["vcf", "vcf_idx"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("vcf",               is_required=True)
        self.add_argument("ref",                is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=2)
        self.add_argument("mem",                is_required=True, default_value=13)

    def define_output(self):
        # Declare merged GVCF output filename
        vcf = self.generate_unique_file_name(extension=".vcf")
        self.add_output("vcf", vcf)
        # Declare GVCF index output filename
        vcf_idx = vcf + ".idx"
        self.add_output("vcf_idx", vcf_idx)

    def define_command(self):
        # Obtaining the arguments
        vcf_in  = self.get_argument("vcf")
        gatk    = self.get_argument("gatk")
        ref     = self.get_argument("ref")
        mem     = self.get_argument("mem")
        vcf_out = self.get_output("vcf")

        # Generate command with java if not running on docker
        if not self.is_docker:
            java = self.get_argument("java")
            jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem * 4 / 5, "/tmp/")
            cmd = "%s %s -cp %s org.broadinstitute.gatk.tools.CatVariants" % (java, jvm_options, gatk)

        # Generate base command with endpoint provided by docker
        else:
            cmd = "%s org.broadinstitute.gatk.tools.CatVariants" % gatk

        # Generating the CatVariants options
        opts = list()
        opts.append("-out %s" % vcf_out)
        opts.append("-R %s" % ref)
        opts.append("-V %s" % vcf_in)

        # Generating the IndexVCF cmd
        return "%s  %s !LOG3!" % (cmd, " ".join(opts))

class FilterMutectCalls(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(FilterMutectCalls, self).__init__(module_id, is_docker)
        self.output_keys    = ["vcf"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("vcf",        is_required=True)
        self.add_argument("nr_cpus",    is_required=True,   default_value=1)
        self.add_argument("mem",        is_required=True,   default_value=2)

    def define_output(self):
        # Declare recoded VCF output filename
        vcf_out = self.generate_unique_file_name(extension=".vcf")
        self.add_output("vcf", vcf_out)

    def define_command(self):
        # Get input arguments
        vcf_in      = self.get_argument("vcf")
        gatk_cmd    = self.get_gatk_command()
        vcf_out     = self.get_output("vcf")
        return "{0} FilterMutectCalls -V {1} -O {2} !LOG3!".format(gatk_cmd, vcf_in, vcf_out)

class CollectReadCounts(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(CollectReadCounts, self).__init__(module_id, is_docker)
        self.output_keys = ["read_count_out"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("bam",            is_required=True)
        self.add_argument("bam_idx",        is_required=True)
        self.add_argument("nr_cpus",        is_required=True,   default_value=1)
        self.add_argument("mem",            is_required=True,   default_value=2)
        self.add_argument("interval_list",  is_required=False)

    def define_output(self):
        # Declare recoded VCF output filename
        read_count_out = self.generate_unique_file_name(extension=".read_count.txt")
        self.add_output("read_count_out", read_count_out)

    def define_command(self):
        # Get input arguments
        bam             = self.get_argument("bam")
        gatk_cmd        = self.get_gatk_command()
        read_count_out  = self.get_output("read_count_out")
        interval_list   = self.get_argument("interval_list")

        cmd = "{0} CollectReadCounts -I {1} -O {2} --format TSV ".format(gatk_cmd, bam, read_count_out)

        if interval_list is not None:
            cmd = "{0} -L {1} --interval-merging-rule OVERLAPPING_ONLY".format(cmd, interval_list)

        return "{0} !LOG3!".format(cmd)

class BedToIntervalList(_GATKBase):
    def __init__(self, module_id, is_docker=False):
        super(BedToIntervalList, self).__init__(module_id, is_docker)
        self.output_keys = ["interval_list"]

    def define_input(self):
        self.add_argument("bed",        is_required=True, is_resource=True)
        self.add_argument("dict_file",  is_required=True, is_resource=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=1)
        self.add_argument("mem",        is_required=True, default_value=2)

    def define_output(self):
        # Declare recoded VCF output filename
        interval_list = self.generate_unique_file_name(extension=".interval.list")
        self.add_output("interval_list", interval_list)

    def define_command(self):

        # Get input arguments
        bed             = self.get_argument("bed")
        dict_file       = self.get_argument("dict_file")
        gatk_cmd        = self.get_gatk_command()
        interval_list   = self.get_output("interval_list")

        return "{0} BedToIntervalList -I {1} -O {2} -SD {3} !LOG3!".format(gatk_cmd, bed, interval_list, dict_file)
