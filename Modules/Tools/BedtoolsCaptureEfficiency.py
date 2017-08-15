from Modules import Module

class BedtoolsCaptureEfficiency(Module):

    def __init__(self, module_id):
        super(BedtoolsCaptureEfficiency, self).__init__(module_id)

        self.input_keys     = ["bam", "bam_idx", "bedtools", "samtools",
                               "target_bed", "subsample_perc", "nr_cpus", "mem"]

        self.output_keys    = ["capture_bed"]

    def define_input(self):
        self.add_argument("bam",            is_required=True)
        self.add_argument("bam_idx",        is_required=True)
        self.add_argument("target_bed",     is_required=True, is_resource=True)
        self.add_argument("samtools",       is_required=True, is_resource=True)
        self.add_argument("bedtools",       is_required=True, is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=1)
        self.add_argument("mem",            is_required=True, default_value=12)
        self.add_argument("subsample_perc", is_required=True, default_value=0.25)

    def define_output(self, platform, split_name=None):
        # Declare capture bed output filename
        capture_bed = self.generate_unique_file_name(split_name=split_name, extension="capture.out")
        self.add_output(platform, "capture_bed", capture_bed)

        # Declare bedtools genome filename
        genome_file = self.generate_unique_file_name(split_name=split_name, extension="bedtools.genome")
        self.add_output(platform, "genome_file", genome_file)

    def define_command(self, platform):
        # Get command to run bedtools intersect to determine capture efficiency of a bam
        # Capture efficiency: Percent of reads overalapping a set of regions

        # Get input arguments
        bam                 = self.get_arguments("bam").get_value()
        subsample_perc      = self.get_arguments("subsample_perc").get_value()
        samtools            = self.get_arguments("samtools").get_value()
        bedtools            = self.get_arguments("bedtools").get_value()
        target_bed          = self.get_arguments("target_bed").get_value()

        # Get output file names
        genome_file         = self.get_output("genome_file")
        capture_bed         = self.get_output("capture_bed")

        # Make genome file to specify sort order of chromosomes in BAM
        make_genome_file_cmd = "%s idxstats %s | awk 'BEGIN{OFS=\"\\t\"}{print $1,$2}' > %s !LOG2!" % \
                               (samtools, bam, genome_file)

        # Case: Run bedtools intersect on subsampled bam
        if (subsample_perc < 1.0) and (subsample_perc > 0.0):
            # generate command for subsampling bam file
            subsample_cmd = "%s view -s %f -b %s" % (samtools, subsample_perc, bam)
            intersect_cmd = "%s intersect -a stdin -b %s -c -sorted -bed -g %s > %s !LOG2!" \
                            % (bedtools, target_bed, genome_file, capture_bed)
            intersect_cmd = subsample_cmd + " | " + intersect_cmd

        # Case: Run bedtools intersect on full bam
        else:
            intersect_cmd = "%s intersect -a %s -b %s -c -sorted -bed -g %s > %s !LOG2!" \
                            % (bedtools, bam, target_bed, genome_file, capture_bed)

        intersect_cmd = "%s ; %s" % (make_genome_file_cmd, intersect_cmd)
        return intersect_cmd
