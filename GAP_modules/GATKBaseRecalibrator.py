import logging

__main_class__="GATKBaseRecalibrator"

class GATKBaseRecalibrator(object):

    def __init__(self, config, sample_data):
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

        self.output_path = None
        self.pipeline_output_path = None

    def get_pipeline_output(self):
        return self.pipeline_output_path

    def get_output(self):
        return self.output_path

    def get_chrom_locations(self, max_nr_reads=2*10**7):

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
        for line in out:
            data = line.strip("\n").split()

            # If we reached the special chromosomes or the unaligned reads, get the all locations for analysis
            if data[0] not in self.sample_data["chrom_list"]:
                break

            chrom_list.append(data[0])
            total += int(data[2])

            # If we reached more than 20 million reads, then return the current available list
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
        recalib_bam = "%s_recalibrated.bam" % bam_prefix
        recalib_bam_idx = "%s_recalibrated.bai" % bam_prefix
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
            opts.append("-L \"%s\"" % ",".join(chrom_list))

        # Generating command for base recalibration
        br_cmd = "%s %s -jar %s -T BaseRecalibrator %s !LOG3!" % (self.java, jvm_options, self.GATK, " ".join(opts))

        # Generating the print reads options
        opts = list()
        opts.append("-I %s" % self.bam)
        opts.append("-o %s" % recalib_bam)
        opts.append("-nct %d" % self.threads)
        opts.append("-R %s" % self.ref)
        opts.append("-BQSR %s" % recalib_report)

        # Generating command for recalibrating the BAM file
        pr_cmd = "%s -Xmx%dG -jar %s -T PrintReads %s !LOG3!" % (self.java, self.mem * 4 / 5, self.GATK, " ".join(opts))

        # Generating the output path
        self.sample_data["bam"] = recalib_bam
        self.sample_data["bam_index"] = recalib_bam_idx
        self.output_path = recalib_bam
        self.pipeline_output_path = recalib_report

        return "%s && %s" % (br_cmd, pr_cmd)