import os

from GAP_interfaces import Tool

__main_class__ = "BedtoolsCaptureEfficiency"

class BedtoolsCaptureEfficiency(Tool):

    def __init__(self, platform, tool_id):
        super(BedtoolsCaptureEfficiency, self).__init__(platform, tool_id)

        self.can_split      = False

        self.nr_cpus        = self.main_server_nr_cpus
        self.mem            = self.main_server_mem

        # I/O keys
        self.input_keys     = ["bam"]
        self.output_keys    = ["capture_bed"]

        # Required tools and resources
        self.req_tools      = ["bedtools", "samtools"]
        self.req_resources  = ["target_bed"]

    def get_command(self, **kwargs):

        # Get command for running bedtools intersect to determine capture efficiency for a bam over a set of BED-formatted targets
        # Obtaining the arguments
        bam                = kwargs.get("bam",                 None)
        subsample_perc     = kwargs.get("subsample_perc",      0.25)
        samtools           = kwargs.get("samtools",            self.tools["samtools"])
        bedtools           = kwargs.get("bedtools",            self.tools["bedtools"])
        target_bed         = kwargs.get("target_bed",          self.resources["target_bed"])

        # Make genome file to specify sort order of chromosomes in BAM
        make_genome_file_cmd = "%s idxstats %s | awk 'BEGIN{OFS=\"\\t\"}{print $1,$2}' > %s !LOG2!" % \
                               (samtools, bam, self.output["genome_file"])

        # Case: Run bedtools intersect on subsampled bam
        if (subsample_perc < 1.0) and (subsample_perc > 0.0):
            # generate command for subsampling bam file
            subsample_cmd = "%s view -s %f -b %s" % (samtools, subsample_perc, bam)
            intersect_cmd = "%s intersect -a stdin -b %s -c -sorted -bed -g %s > %s !LOG2!" \
                            % (bedtools, target_bed, self.output["genome_file"], self.output["capture_bed"])
            intersect_cmd = subsample_cmd + " | " + intersect_cmd

        # Case: Run bedtools intersect on full bam
        else:
            intersect_cmd = "%s intersect -a %s -b %s -c -sorted -bed -g %s > %s !LOG2!" \
                            % (bedtools, bam, target_bed, self.output["genome_file"], self.output["capture_bed"])

        intersect_cmd = "%s ; %s" % (make_genome_file_cmd, intersect_cmd)

        return intersect_cmd

    def init_output_file_paths(self, **kwargs):
        self.generate_output_file_path("capture_bed", "capture.out")
        self.generate_output_file_path("genome_file", "bedtools.genome")

