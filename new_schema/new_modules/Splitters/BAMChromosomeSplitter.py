import logging
import operator
from Module import Module

class BAMChromosomeSplitter(Module):

    def __init__(self, module_id):
        super(BAMChromosomeSplitter, self).__init__(module_id)

        self.input_keys  = ["bam", "bam_idx", "samtools", "nr_splits"]
        self.output_keys = ["bam", "is_aligned", "chroms"]

    def define_input(self):
        self.add_argument("bam",                is_required=True)
        self.add_argument("bam_idx",            is_required=True)
        self.add_argument("samtools",           is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default=8)
        self.add_argument("mem",                is_required=True, default=16)
        self.add_argument("nr_splits",          is_required=True, default=23)

    def define_output(self, platform, split_name=None):
        # Obtaining the arguments
        bam         = self.get_arguments("bam").get_value()
        samtools    = self.get_arguments("samtools").get_value()
        nr_splits   = self.get_arguments("nr_splits").get_value()

        # Obtaining chromosome data from bam header
        try:
            logging.info("BAMChromosomeSplitter determining chromosomes to use for splits...")
            chroms, remains = self.get_chrom_splits(platform, samtools, bam, nr_splits)
        except:
            logging.error("BAMChromosomeSplitter unable to determine chromosomes to use for splits!")
            raise

        # Add split info for named chromosomes
        for chrom in chroms:
            split_info = {"split_name"  : chrom,
                          "chroms"      : chrom,
                          "is_aligned"  : True,
                          "bam"         : self.generate_unique_filename(split_name=chrom, extension=".bam")}
            self.add_output(platform, chrom, split_info)

        # Add split info for all chromosomes that aren't named in config
        split_info   = {"split_name"    : "remains",
                        "chroms"        : remains,
                        "is_aligned"    : True,
                        "bam"           : self.generate_unique_filename(split_name="remains", extension=".bam")}
        self.add_output(platform, "remains", split_info)

        # Add split info for unmapped reads
        split_info = {"split_name"  : "unmapped",
                      "chroms"      : None,
                      "is_aligned"  : False,
                      "bam"         : self.generate_unique_filename(split_name="unmapped", extension=".bam")}
        self.add_output(platform, "unmapped", split_info)

    def define_command(self, platform):
        # Obtaining the arguments
        bam         = self.get_arguments("bam").get_value()
        samtools    = self.get_arguments("samtools").get_value()
        nr_cpus     = self.get_arguments("nr_cpus").get_value()

        # Get names of chromosomes
        chroms  = [split["chroms"] for split in self.output if split["split_name"] not in ["remains", "unmapped"]]
        remains = [split["chroms"] for split in self.output if split["split_name"] == "remains"][0]

        # Get output file basename
        split_name      = self.output.keys()[0]
        output_basename = self.output[split_name]["bam"].split(split_name)[0]

        # Generating the commands
        cmds = list()

        # Obtaining the chromosomes in parallel
        cmd = '%s view -@ %d -u -F 4 %s $chrom_name > %s$chrom_name.bam' % (samtools, nr_cpus, bam, output_basename)
        cmds.append('for chrom_name in %s; do %s & done' % (" ".join(chroms), cmd))

        # Obtaining the remaining chromosomes from the bam header
        cmds.append('%s view -@ %d -u -F 4 %s %s > %sremains.bam'
                    % (samtools, nr_cpus, bam, " ".join(remains), output_basename))

        # Obtaining the unaligned reads
        cmds.append('%s view -@ %d -u -f 4 %s > %sunmapped.bam'
                    % (samtools, nr_cpus, bam, output_basename))

        # Parallel split of the files
        return "%s ; wait" % " & ".join(cmds)

    @staticmethod
    def get_chrom_splits(platform, samtools, bam, nr_splits):
        # Returns two lists, one containing the names of chromosomes which will be considered separate splits
        # And another containing the names of chromosomes that will be lumped together an considered one split
        # Split chromosomes will be determined by the number of reads mapped to each chromosome

        # Obtaining the chromosome alignment information
        cmd = "%s idxstats %s" % (samtools, bam)
        out, err = platform.run_quick_command("bam_splitter_idxstats", cmd)

        # Parse output to get number of reads mapping to each chromosome
        chrom_data  = dict()
        remains     = list()
        for line in out.split("\n"):
            data = line.split()
            # Skip unmapped reads and empty entries at the end of the file
            if (len(data) > 0) and (data[0] != "*"):
                # Skip chromosomes with 0 reads mapped
                if int(data[2]) > 0:
                    # Add name of next chromosome
                    remains.append(data[0])
                    # Add data for next chromosome
                    chrom_data[data[0]] = int(data[2])

        # Get chromosome names sorted by number of reads mapping to each
        sorted_chrom_names = [x[0] for x in sorted(chrom_data.items(), key=operator.itemgetter(1), reverse=True)]

        # Create list of split and lumped chromosome by adding chromosomes in order of size until number of splits is reached
        chroms = list()
        i = 0
        while (i < nr_splits-1) and (len(remains) > 0):
            # Create split for next largest chromosome if any more chromosomes are left
            chroms.append(sorted_chrom_names[i])
            # Remove chromosome name from list of chromosomes to be lumped together
            remains.remove(sorted_chrom_names[i])
            i += 1

        return chroms, remains
