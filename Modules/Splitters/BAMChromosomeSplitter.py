import logging
import operator

from GAP_interfaces import Splitter

__main_class__ = "BAMChromosomeSplitter"

class BAMChromosomeSplitter(Splitter):

    def __init__(self, platform, tool_id, main_module_name=None):
        super(BAMChromosomeSplitter, self).__init__(platform, tool_id, main_module_name)

        self.nr_cpus     = self.main_server_nr_cpus
        self.mem         = self.main_server_mem

        self.input_keys  = ["bam", "bam_idx"]
        self.output_keys = ["bam", "is_aligned"]

        self.req_tools      = ["samtools"]
        self.req_resources  = []

        # Number of splits BAM file will be divided among
        self.nr_chrom_splits = self.config["general"]["nr_splits"]

    def init_split_info(self, **kwargs):
        # Obtaining the arguments
        bam           = kwargs.get("bam",           None)

        # Obtaining chromosome data from bam header
        chroms, remains = self.get_chrom_splits(bam)

        # Add split info for named chromosomes
        for chrom in chroms:
            split_info = {"split_name"  : chrom,
                          "chroms"      : chrom,
                          "is_aligned"  : True,
                          "bam"         : None}
            self.output.append(split_info)

        # Add split info for all chromosomes that aren't named in config
        split_info   = {"split_name"    : "remains",
                        "chroms"        : remains,
                        "is_aligned"    : True,
                        "bam"           : None}
        self.output.append(split_info)

        # Add split info for unmapped reads
        split_info = {"split_name"  : "unmapped",
                      "chroms"      : None,
                      "is_aligned"  : False,
                      "bam"         : None}
        self.output.append(split_info)

    def init_output_file_paths(self, **kwargs):
        for i in range(len(self.output)):
            split_id    = i
            split_name  = self.output[i]["split_name"]
            self.generate_output_file_path(output_key="bam",
                                           extension="bam",
                                           split_id=split_id,
                                           split_name=split_name)

    def get_command(self, **kwargs):

        # Obtaining the arguments
        bam            = kwargs.get("bam",             None)
        nr_cpus        = kwargs.get("nr_cpus",         self.nr_cpus)

        # Get names of chromosomes
        chroms  = [split["chroms"] for split in self.output if split["split_name"] not in ["remains", "unmapped"]]
        remains = [split["chroms"] for split in self.output if split["split_name"] == "remains"][0]

        # Get names of output files
        chrm_output_basename    = self.output[0]["bam"].split(self.output[0]["split_name"])[0]
        remains_output          = [split["bam"] for split in self.output if split["split_name"] == "remains"][0]
        unmapped_output         = [split["bam"] for split in self.output if split["split_name"] == "unmapped"][0]

        # Generating the commands
        cmds = list()

        # Obtaining the chromosomes in parallel
        cmd = '%s view -@ %d -u -F 4 %s $chrom_name > %s$chrom_name.bam' % (self.tools["samtools"], nr_cpus, bam, chrm_output_basename)
        cmds.append('for chrom_name in %s; do %s & done' % (" ".join(chroms), cmd))

        # Obtaining the remaining chromosomes from the bam header
        cmds.append('%s view -@ %d -u -F 4 %s %s > %s'
                    % (self.tools["samtools"], nr_cpus, bam, " ".join(remains), remains_output))

        # Obtaining the unaligned reads
        cmds.append('%s view -@ %d -u -f 4 %s > %s'
                    % (self.tools["samtools"], nr_cpus, bam, unmapped_output))

        # Parallel split of the files
        return "%s ; wait" % " & ".join(cmds)

    def get_chrom_splits(self, bam):
        # Returns two lists, one containing the names of chromosomes which will be considered separate splits
        # And another containing the names of chromosomes that will be lumped together an considered one split
        # Split chromosomes will be determined by the number of reads mapped to each chromosome

        # Obtaining the chromosome alignment information
        main_instance = self.platform.get_main_instance()
        cmd = "%s idxstats %s" % (self.tools["samtools"], bam)
        main_instance.run_command("bam_splitter_idxstats", cmd, log=False)
        out, err = main_instance.get_proc_output("bam_splitter_idxstats")

        if err != "":
            err_msg = "Could not obtain information for %s. " % self.main_module_name
            err_msg += "The following command was run: \n  %s. " % cmd
            err_msg += "The following error appeared: \n  %s." % err
            logging.error(err_msg)
            exit(1)

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
        while (i < self.nr_chrom_splits-1) and (len(remains) > 0):
            # Create split for next largest chromosome if any more chromosomes are left
            chroms.append(sorted_chrom_names[i])
            # Remove chromosome name from list of chromosomes to be lumped together
            remains.remove(sorted_chrom_names[i])
            i += 1

        return chroms, remains
