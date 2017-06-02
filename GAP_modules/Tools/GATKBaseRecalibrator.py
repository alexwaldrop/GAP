import logging

from GAP_interfaces import Tool

__main_class__="GATKBaseRecalibrator"

class GATKBaseRecalibrator(Tool):

    def __init__(self, platform, tool_id):
        super(GATKBaseRecalibrator, self).__init__(platform, tool_id)

        self.can_split      = False

        self.nr_cpus        = self.config["platform"]["MS_nr_cpus"]
        self.mem            = self.config["platform"]["MS_mem"]

        self.input_keys     = ["bam"]
        self.output_keys    = ["BQSR_report"]

        self.req_tools      = ["gatk", "java", "samtools"]
        self.req_resources  = ["ref", "dbsnp"]

    def get_chrom_locations(self, bam, max_nr_reads=2.5*10**7):

        # Obtaining the chromosome alignment information
        main_instance = self.platform.get_main_instance()
        cmd = "%s idxstats %s" % (self.tools["samtools"], bam)
        main_instance.run_command("bam_idxstats", cmd, log=False)
        out, err = main_instance.get_proc_output("bam_idxstats")

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
            if data[0] not in self.config["sample"]["chrom_list"]:
                break

            chrom_list.append(data[0])
            total += int(data[2])

            # If we reached more than maximum number reads, then return the current available list
            if total >= int(max_nr_reads):
                return chrom_list

        # If here, then process the entire file
        return None

    def get_command(self, **kwargs):
        # Obtaining the arguments
        bam            = kwargs.get("bam",         None)
        nr_cpus        = kwargs.get("nr_cpus",     self.nr_cpus)
        mem            = kwargs.get("mem",         self.mem)

        # Generating variables
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem*4/5, self.tmp_dir)

        # Generating the base recalibration options
        opts = list()
        opts.append("-I %s" % bam)
        opts.append("-o %s" % self.output["BQSR_report"])
        opts.append("-nct %d" % nr_cpus)
        opts.append("-R %s" % self.resources["ref"])
        opts.append("-knownSites %s" % self.resources["dbsnp"])
        opts.append("-cov ReadGroupCovariate")
        opts.append("-cov QualityScoreCovariate")
        opts.append("-cov CycleCovariate")
        opts.append("-cov ContextCovariate")

        # Limit the number of reads processed
        chrom_list = self.get_chrom_locations(bam)
        if chrom_list is not None:
            for chrom in chrom_list:
                opts.append("-L \"%s\"" % chrom)

        # Generating command for base recalibration
        br_cmd = "%s %s -jar %s -T BaseRecalibrator %s !LOG3!" % (self.tools["java"], jvm_options, self.tools["gatk"], " ".join(opts))

        return br_cmd

    def init_output_file_paths(self, **kwargs):

        self.generate_output_file_path(output_key="BQSR_report",
                                       extension=".grp")
