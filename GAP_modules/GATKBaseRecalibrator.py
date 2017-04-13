import logging

from GAP_interfaces import Tool

__main_class__="GATKBaseRecalibrator"

class GATKBaseRecalibrator(Tool):

    def __init__(self, config, sample_data):
        super(GATKBaseRecalibrator, self).__init__()

        self.config = config
        self.sample_data = sample_data

        self.java = self.config["paths"]["java"]
        self.GATK = self.config["paths"]["gatk"]

        self.ref = self.config["paths"]["ref"]
        self.dbsnp = self.config["paths"]["dbsnp"]

        self.temp_dir = self.config["general"]["temp_dir"]

        self.can_split = False

        self.bam = None
        self.threads = None



    def get_chrom_locations(self, max_nr_reads=2.5*10**7):

        # Obtaining the chromosome alignment information
        cmd = "samtools idxstats %s" % self.bam
        out, err = self.sample_data["main-server"].run_command("bam_idxstats", cmd, log=False, get_output=True)
        if err != "":
            err_msg = "Could not obtain information for BaseRecalibrator. "
            err_msg += "The following command was run: \n  %s. " % cmd
            err_msg += "The following error appeared: \n  %s." % err
            logging.error(err_msg)
            exit(1)

        # Analysing the output of idxstats to identify which chromosome location is needed we need
        chrom_list = list()
        total = 0
        for line in out.split("\n"):
            if len(line) == 0:
                continue

            data = line.split()

            # If we reached the special chromosomes or the unaligned reads, get all the locations for analysis
            if data[0] not in self.sample_data["chrom_list"]:
                break

            chrom_list.append(data[0])
            total += int(data[2])

            # If we reached more than maximum number reads, then return the current available list
            if total >= max_nr_reads:
                return chrom_list

        # If here, then process the entire file
        return None

    def get_command(self, **kwargs):
        # Obtaining the arguments
        self.bam            = kwargs.get("bam", self.sample_data["bam"])
        self.threads        = kwargs.get("cpus", self.config["instance"]["nr_cpus"])
        self.mem            = kwargs.get("mem", self.config["instance"]["mem"])

        # Generating variables
        bam_prefix = self.bam.split(".")[0]
        recalib_report = "%s_BQSR.grp" % bam_prefix
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=/data/tmp" % (self.mem*4/5)

        # Generating the base recalibration options
        opts = list()
        opts.append("-I %s" % self.bam)
        opts.append("-o %s" % recalib_report)
        opts.append("-nct %d" % self.threads)
        opts.append("-R %s" % self.ref)
        opts.append("-knownSites %s" % self.dbsnp)
        opts.append("-cov ReadGroupCovariate")
        opts.append("-cov QualityScoreCovariate")
        opts.append("-cov CycleCovariate")
        opts.append("-cov ContextCovariate")

        # Limit the number of reads processed
        chrom_list = self.get_chrom_locations()
        if chrom_list is not None:
            for chrom in chrom_list:
                opts.append("-L \"%s\"" % chrom)

        # Generating command for base recalibration
        br_cmd = "%s %s -jar %s -T BaseRecalibrator %s !LOG3!" % (self.java, jvm_options, self.GATK, " ".join(opts))

        # Generating the output path
        self.sample_data["BQSR_report"] = recalib_report
        self.final_output = recalib_report

        return br_cmd