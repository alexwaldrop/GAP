import logging

from Modules import Module

class ChrReferenceSplitter(Module):

    def __init__(self, module_id):
        super(ChrReferenceSplitter, self).__init__(module_id)

        self.input_keys     = ["chr_list", "nr_splits", "nr_cpus", "mem"]
        self.output_keys    = ["location", "excluded_location"]

    def define_input(self):
        self.add_argument("chr_list",       is_required=True,   is_resource=True)
        self.add_argument("nr_splits",      is_required=True,   default_value=25)
        self.add_argument("nr_cpus",        is_required=True,   default_value=1)
        self.add_argument("mem",            is_required=True,   default_value=1)

    def define_output(self, platform, split_name=None):
        # Obtain arguments
        chr_list    = self.get_arguments("chr_list").get_value()
        nr_splits   = self.get_arguments("nr_splits").get_value()

        # Get information related to each split
        # Process each chromosome separately and process the rest in one single run
        try:
            logging.info("ChrReferenceSplitter determining chromosomes to use for splits...")
            chrom_list, remains = self.__get_chrom_splits(platform, chr_list, nr_splits)
        except:
            logging.error("ChrReferenceSplitter unable to determine chromosomes to use for splits!")
            raise

        # Add split for each chromosome
        for chrom in chrom_list:
            split_name = chrom
            split_data = {
                    "location": chrom,
                    "excluded_location": None}
            self.add_output(platform, split_name, split_data, is_path=False)

        # Add data for final split (if one exists)
        # If num_splits is > num_chrom remains will be a list one element: ['unmapped'])
        if len(remains) > 1:
            split_name = "remains"
            split_data = {
                "location": remains,
                "excluded_location": None}
            self.add_output(platform, split_name, split_data, is_path=False)

    def define_command(self, platform):
        # No command needs to be run
        return None

    @staticmethod
    def __get_chrom_splits(platform, chr_list, nr_splits):
        # Returns two lists, one containing the names of chromosomes which will be considered separate splits
        # And another containing the names of chromosomes that will be lumped together an considered one split
        # Split chromosomes will be determined by the number of set splits

        cmd = "cat %s" % chr_list
        out, err = platform.run_quick_command("chr_list_splitter", cmd)

        # Create the list of chromosomes
        chrom_names = list()
        for line in out.split("\n"):
            # Skip empty lines
            if len(line) == 0:
                continue

            # Add name of next chromosome
            chrom_names.append(line)

        # Create list of split and lumped chromosome by adding chromosomes until number of splits is reached
        chroms = list()
        i = 0
        while (i < nr_splits-1) and (len(chrom_names) > 0):
            # Create split for next largest chromosome if any more chromosomes are left
            chroms.append(chrom_names.pop(0))
            i += 1
        remains = chrom_names

        # Add unmapped reads
        remains.append("unmapped")
        return chroms, remains
