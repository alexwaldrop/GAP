import logging

from GAP_interfaces import Splitter

__main_class__ = "BAMChromosomeSplitter"

class BAMChromosomeSplitter(Splitter):

    def __init__(self, config, sample_data, tool_id, main_module_name=None):
        super(BAMChromosomeSplitter, self).__init__(config, sample_data, tool_id, main_module_name)

        self.nr_cpus     = self.main_server_nr_cpus
        self.mem         = self.main_server_mem

        self.input_keys  = ["bam"]
        self.output_keys = ["bam", "is_aligned"]

        self.req_tools      = ["samtools"]
        self.req_resources  = []

    def get_header(self, bam):

        # Obtain the reference sequences IDs
        cmd = "%s view -H %s | grep \"@SQ\"" % (self.tools["samtools"], bam)
        self.sample_data["main-server"].run_command("bam_header", cmd, log=False)
        out, err = self.sample_data["main-server"].get_proc_output("bam_header")

        if err != "":
            err_msg = "Could not obtain the header from the BAM file. "
            err_msg += "\nThe following command was run: \n  %s " % cmd
            err_msg += "\nThe following error appeared: \n  %s" % err
            logging.error(err_msg)
            exit(1)

        # Obtain the references that are marked in the config file
        chrom_list_config = self.sample_data["chrom_list"]
        ref_in_config = list()
        ref_not_in_config = list()
        for line in out.split("\n"):
            # Skip empty lines
            if len(line) < 3:
                continue

            sequence_name = line.split()[1]

            # Clear "SN:" from the beginning of the sequence
            sequence_id = sequence_name.split(":", 1)[1]
            if sequence_id in chrom_list_config:
                ref_in_config.append(sequence_id)
            else:
                ref_not_in_config.append(sequence_id)

        if not ref_in_config:
            logging.error("The current bam file (%s) does not contain any reference defined in the config file." % bam)
            exit(1)

        return ref_in_config, ref_not_in_config

    def init_split_info(self, **kwargs):
        # Obtaining the arguments
        bam = kwargs.get("bam", None)

        # Obtaining chromosome data from bam header
        chroms, remains = self.get_header(bam)

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
            self.generate_output_file_path("bam", "bam", split_id=split_id, split_name=split_name)

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

