import logging

from Modules import Module

class GATKBaseRecalibrator(Module):

    def __init__(self, module_id):
        super(GATKBaseRecalibrator, self).__init__(module_id)

        self.output_keys    = ["BQSR_report"]

    def define_input(self):
        self.add_argument("bam",            is_required=True)
        self.add_argument("bam_idx",        is_required=True)
        self.add_argument("chrom_list",     is_required=False)
        self.add_argument("gatk",           is_required=True, is_resource=True)
        self.add_argument("java",           is_required=True, is_resource=True)
        self.add_argument("samtools",       is_required=True, is_resource=True)
        self.add_argument("ref",            is_required=True, is_resource=True)
        self.add_argument("dbsnp",          is_required=True, is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value="MAX")
        self.add_argument("mem",            is_required=True, default_value="nr_cpus * 2")
        self.add_argument("max_nr_reads",   is_required=True, default_value=2.5*10**7)

    def define_output(self):
        # Declare BQSR report file
        bqsr_report = self.generate_unique_file_name(extension=".grp")
        self.add_output("BQSR_report", bqsr_report)

    def define_command(self):
        # Get arguments needed to generate GATK BQSR command
        bam             = self.get_argument("bam")
        gatk            = self.get_argument("gatk")
        java            = self.get_argument("java")
        samtools        = self.get_argument("samtools")
        chrom_list      = self.get_argument("chrom_list")
        ref             = self.get_argument("ref")
        dbsnp           = self.get_argument("dbsnp")
        nr_cpus         = self.get_argument("nr_cpus")
        mem             = self.get_argument("mem")
        max_nr_reads    = self.get_argument("max_nr_reads")

        # Convert max_nr_reads to integer if necessary
        max_nr_reads    = eval(max_nr_reads) if isinstance(max_nr_reads, basestring) else max_nr_reads

        # Get output file
        bqsr_report     = self.get_output("BQSR_report")

        # Generate JVM options
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=/tmp/" % (mem*4/5)

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
        #try:
            #logging.info("Determining chromosomes to include for BQSR...")
            #chrom_list = GATKBaseRecalibrator.__get_chrom_locations(platform, bam, max_nr_reads, samtools)
        #except:
        #    logging.error("Unable to determine the number of chromosomes for BQSR!")
        #    raise

        # Add the minimum amount of chromosomes to exceed the max_read_nr
        if chrom_list is not None:
            for chrom in chrom_list:
                opts.append("-L \"%s\"" % chrom)

        # Generating command for base recalibration
        cmd = "%s %s -jar %s -T BaseRecalibrator %s !LOG3!" % (java,
                                                               jvm_options,
                                                               gatk,
                                                               " ".join(opts))
        return cmd

    @staticmethod
    def __get_chrom_locations(platform, bam, max_nr_reads, samtools):
        # Obtaining the chromosome alignment information
        cmd = "%s idxstats %s" % (samtools, bam)
        out, err = platform.run_quick_command("bam_idxstats", cmd)

        # Analysing the output of idxstats to identify which chromosome location is needed we need
        chrom_list = list()
        total = 0
        for line in out.split("\n"):
            if len(line) == 0:
                continue

            data = line.split()
            if data[0] != "*":
                chrom_list.append(data[0])
                total += int(data[2])

            # If we reached more than maximum number reads, then return the current available list
            if total >= int(max_nr_reads):
                return chrom_list

        # If here, then process the entire file
        return None


