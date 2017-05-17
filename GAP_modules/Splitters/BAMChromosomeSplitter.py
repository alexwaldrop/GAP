import logging

from GAP_interfaces import Splitter

__main_class__ = "BAMChromosomeSplitter"

class BAMChromosomeSplitter(Splitter):

    def __init__(self, config, sample_data):
        super(BAMChromosomeSplitter, self).__init__(config, sample_data)

        self.nr_cpus     = self.config["platform"]["MS_nr_cpus"]
        self.mem         = self.config["platform"]["MS_mem"]

        self.input_keys  = ["bam"]
        self.output_keys = ["bam", "is_aligned"]

        self.req_tools      = ["samtools"]
        self.req_resources  = []

        self.bam = None

    def get_header(self):

        # Obtain the reference sequences IDs
        cmd = "%s view -H %s | grep \"@SQ\"" % (self.tools["samtools"], self.bam)
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
            logging.error("The current bam file (%s) does not contain any reference defined in the config file." % self.bam)
            exit(1)

        return ref_in_config, ref_not_in_config

    def get_command(self, **kwargs):

        # Obtaining the arguments
        self.bam            = kwargs.get("bam",             None)
        self.nr_cpus        = kwargs.get("nr_cpus",         self.nr_cpus)
        self.mem            = kwargs.get("mem",             self.mem)

        bam_prefix  = self.bam.split(".")[0]

        # Obtaining chromosome data from bam header
        chroms, remains = self.get_header()

        # Generating the commands
        cmds = list()

        # Obtaining the chromosomes in parallel
        cmd = '%s view -@ %d -u -F 4 %s $chrom_name > %s_$chrom_name.bam' % (self.tools["samtools"], self.nr_cpus, self.bam, bam_prefix)
        cmds.append('for chrom_name in %s; do %s & done' % (" ".join(chroms), cmd))

        # Obtaining the remaining chromosomes from the bam header
        cmds.append('%s view -@ %d -u -F 4 %s %s > %s_remains.bam' % (self.tools["samtools"], self.nr_cpus, self.bam, " ".join(remains), bam_prefix))

        # Obtaining the unaligned reads
        cmds.append('%s view -@ %d -u -f 4 %s > %s_unmaped.bam' % (self.tools["samtools"], self.nr_cpus, self.bam, bam_prefix))

        # Setting up the output paths
        self.output = list()
        self.output.extend(
            [
                {
                    "bam":          "%s_%s.bam" % (bam_prefix, chrom_name),
                    "is_aligned":   True
                } for chrom_name in chroms
            ]
        )
        self.output.append(
            {
                "bam":          "%s_remains.bam" % bam_prefix,
                "is_aligned":   True
            }
        )
        self.output.append(
            {
                "bam":          "%s_unmaped.bam" % bam_prefix,
                "is_aligned":   False
            }
        )

        # Parallel split of the files
        return "%s ; wait" % " & ".join(cmds)
