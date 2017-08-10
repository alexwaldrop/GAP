import logging
from Module import Module

class GATKReferenceSplitter(Module):

    def __init__(self, module_id):
        super(GATKReferenceSplitter, self).__init__(module_id)

        self.input_keys     = ["bam", "samtools", "nr_splits", "nr_cpus", "mem"]
        self.output_keys    = ["location", "excluded_location"]

    def define_input(self):
        self.add_argument("bam",            is_required=True)
        self.add_argument("samtools",       is_required=True,   is_resource=True)
        self.add_argument("nr_splits",      is_required=True,   default_value=25)
        self.add_argument("nr_cpus",        is_required=True,   default_value=1)
        self.add_argument("mem",            is_required=True,   default_value=1)

    def define_output(self, platform, split_name=None):
        # Obtain arguments
        bam         = self.get_arguments("bam").get_value()
        samtools    = self.get_arguments("samtools").get_value()
        nr_splits   = self.get_arguments("nr_splits").get_value()

        # Get information related to each split
        # Process each chromosome separately and process the rest in one single run
        # Obtaining chromosome data from bam header
        try:
            logging.info("GATKReferenceSplitter determining chromosomes to use for splits...")
            chrom_list, remains = self.__get_chrom_splits(platform, samtools, bam, nr_splits)
        except:
            logging.error("GATKReferenceSplitter unable to determine chromosomes to use for splits!")
            raise

        # Add split for each chromosome
        for chrom in chrom_list:
            split_name = chrom
            split_data = {
                    "location": chrom,
                    "excluded_location": None}
            self.add_output(platform, split_name, split_data, is_path=False)

        # Add data for final split
        split_name = "remains"
        split_data = {
                "location": None,
                "excluded_location": chrom_list}
        self.add_output(platform, split_name, split_data, is_path=False)

    def define_command(self, platform):
        # No command needs to be run
        return None

    @staticmethod
    def __get_chrom_splits(platform, samtools, bam, nr_splits):
        # Returns two lists, one containing the names of chromosomes which will be considered separate splits
        # And another containing the names of chromosomes that will be lumped together an considered one split
        # Split chromosomes will be determined by the number of reads mapped to each chromosome

        # Obtaining the chromosome alignment information
        # Output sorted in descending order by the number of reads mapping to each chromosome
        cmd = "%s idxstats %s | sort -nrk 3" % (samtools, bam)
        out, err = platform.run_quick_command("gatk_bam_splitter_idxstats", cmd)

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
        while (i < nr_splits-1) and (len(sorted_chrom_names) > 0):
            # Create split for next largest chromosome if any more chromosomes are left
            chroms.append(sorted_chrom_names.pop(0))
            i += 1
        remains = sorted_chrom_names
        return chroms, remains
