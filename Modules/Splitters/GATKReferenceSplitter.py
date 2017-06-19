from GAP_interfaces import Splitter
import logging

__main_class__ = "GATKReferenceSplitter"

class GATKReferenceSplitter(Splitter):

    def __init__(self, platform, tool_id, main_module_name=None):
        super(GATKReferenceSplitter, self).__init__(platform, tool_id, main_module_name)

        self.input_keys     = ["bam", "bam_idx"]
        self.output_keys    = ["bam", "BQSR_report", "location", "excluded_location"]

        self.req_tools      = ["samtools"]
        self.req_resources  = []

        # Number of splits BAM file will be divided among
        self.nr_chrom_splits = self.config["general"]["nr_splits"]

    def init_split_info(self, **kwargs):
        # Obtain arguments
        bam = kwargs.get("bam", None)
        BQSR_report = kwargs.get("BQSR_report", None)

        # Get information related to each split
        # Process each chromosome separately and process the rest in one single run
        chrom_list, remains = self.get_chrom_splits(bam)

        for chrom in chrom_list:
            self.output.append(
                {
                    "bam": bam,
                    "BQSR_report": BQSR_report,
                    "location": chrom,
                    "excluded_location": None
                }
            )
        self.output.append(
            {
                "bam": bam,
                "BQSR_report": BQSR_report,
                "location": None,
                "excluded_location": chrom_list
            }
        )

    def init_output_file_paths(self, **kwargs):
        # No output files need to be generated
        return None

    def get_command(self, **kwargs):
        # No command needs to be run
        return None

    def get_chrom_splits(self, bam):
        # Returns two lists, one containing the names of chromosomes which will be considered separate splits
        # And another containing the names of chromosomes that will be lumped together an considered one split
        # Split chromosomes will be determined by the number of reads mapped to each chromosome

        # Obtaining the chromosome alignment information
        # Output sorted in descending order by the number of reads mapping to each chromosome
        main_instance = self.platform.get_main_instance()
        cmd = "%s idxstats %s | sort -nrk 3" % (self.tools["samtools"], bam)
        main_instance.run_command("gatk_bam_splitter_idxstats", cmd, log=False)
        out, err = main_instance.get_proc_output("gatk_bam_splitter_idxstats")

        if err != "":
            err_msg = "Could not obtain information for %s. " % self.main_module_name
            err_msg += "The following command was run: \n  %s. " % cmd
            err_msg += "The following error appeared: \n  %s." % err
            logging.error(err_msg)
            exit(1)

        # Analysing the output of idxstats to identify the number of reads mapping to each chromosome
        sorted_chrom_names = list()
        for line in out.split("\n"):
            # Skip empty lines
            if len(line) == 0:
                continue

            data = line.split()

            # Skip unmapped reads
            if data[0] == "*":
                continue

            # Add name of next chromosome
            sorted_chrom_names.append(data[0])

        # Create list of split and lumped chromosome by adding chromosomes in order of size until number of splits is reached
        chroms = list()
        i = 0
        while (i < self.nr_chrom_splits-1) and (len(sorted_chrom_names) > 0):
            # Create split for next largest chromosome if any more chromosomes are left
            chroms.append(sorted_chrom_names.pop(0))
            i += 1

        remains = sorted_chrom_names
        return chroms, remains
