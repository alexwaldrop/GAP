from Modules import Splitter

class RefSplitter(Splitter):

    def __init__(self, module_id):
        super(RefSplitter, self).__init__(module_id)
        self.output_keys = ["location", "excluded_location"]

    def define_input(self):
        self.add_argument("chrom_list",         is_required=True)
        self.add_argument("nr_splits",          is_required=True,   default_value=25)
        self.add_argument("include_remains",    is_required=True,   default_value=True)
        self.add_argument("nr_cpus",            is_required=True,   default_value=1)
        self.add_argument("mem",                is_required=True,   default_value=1)

    def define_output(self):
        # Obtain arguments
        chr_list        = self.get_argument("chrom_list")
        nr_splits       = self.get_argument("nr_splits")
        include_remains = self.get_argument("include_remains")

        # First nr_splits-1 chromosomes get put into own split
        # Rest get lumped into 'remains' group
        chrom_list, remains = self.__get_chrom_splits(chr_list, nr_splits)

        # Add split for each chromosome
        for chrom in chrom_list:
            self.make_split(split_id=chrom)
            self.add_output(split_id=chrom, key="location", value=chrom, is_path=False)
            self.add_output(split_id=chrom, key="excluded_location", value=None, is_path=False)

        # Add data for final split (if one exists)
        # If num_splits is > num_chrom remains will be a list one element: ['unmapped'])
        if len(remains) > 1 and include_remains:
            self.make_split(split_id="remains")
            self.add_output(split_id="remains", key="location", value=remains, is_path=False)
            self.add_output(split_id="remains", key="excluded_location", value=chrom_list, is_path=False)

    def define_command(self):
        # No command needs to be run
        return None

    @staticmethod
    def __get_chrom_splits(chr_list, nr_splits):
        # Returns two lists, one containing the names of chromosomes which will be considered separate splits
        # And another containing the names of chromosomes that will be lumped together an considered one split
        # Split chromosomes will be determined by the number of reads mapped to each chromosome

        chroms = list()
        i = 0
        while (i < nr_splits - 1) and (len(chr_list) > 0):
            # Create split for next largest chromosome if any more chromosomes are left
            chroms.append(chr_list.pop(0))
            i += 1
        remains = chr_list
        return chroms, remains