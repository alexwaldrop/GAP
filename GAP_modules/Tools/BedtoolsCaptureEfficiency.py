import os

from GAP_interfaces import Tool

__main_class__ = "BedtoolsCaptureEfficiency"

class BedtoolsCaptureEfficiency(Tool):

    def __init__(self, config, sample_data):
        super(BedtoolsCaptureEfficiency, self).__init__(config, sample_data)

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
        is_bam_sorted      = kwargs.get("is_bam_sorted",       True)
        subsample_perc     = kwargs.get("subsample_perc",      0.25)
        samtools           = kwargs.get("samtools",            self.tools["samtools"])
        bedtools           = kwargs.get("bedtools",            self.tools["bedtools"])
        target_bed         = kwargs.get("target_bed",          self.resources["target_bed"])

        # Generate output filename
        bam_prefix = bam.split(".")[0]
        intersect_output = "%s.intersect.txt" % bam_prefix

        # Sort bam if necessary
        sort_bam_cmd = ""
        if not is_bam_sorted:
            # Get new filename for sorted bed
            bam_basename    = os.path.join(self.tmp_dir, bam.split("/")[-1])
            sorted_bam      = "%s.sorted.%s" % (bam_basename.split(".")[0], ".".join(bam_basename.split(".")[1:]))
            # Sort and index bam and set self.bam to the name of the sorted bam
            sort_bam_cmd    = "%s sort %s > %s !LOG2!" % (samtools, bam, sorted_bam)
            index_bam_cmd   = "%s index %s !LOG2!" % (samtools, sorted_bam)
            sort_bam_cmd    = sort_bam_cmd + " ; " + index_bam_cmd + " ; "
            bam        = sorted_bam

        # Make genome file to specify sort order of chromosomes in BAM
        genome_file = "%s.genome" % bam_prefix
        make_genome_file_cmd = "%s idxstats %s | awk 'BEGIN{OFS=\"\\t\"}{print $1,$2}' > %s !LOG2!" % \
                               (samtools, bam, genome_file)

        # Case: Run bedtools intersect on subsampled bam
        if (subsample_perc < 1.0) and (subsample_perc > 0.0):
            # generate command for subsampling bam file
            subsample_cmd = "%s view -s %f -b %s" % (samtools, subsample_perc, bam)
            intersect_cmd = "%s intersect -a stdin -b %s -c -sorted -bed -g %s > %s !LOG2!" \
                            % (bedtools, target_bed, genome_file, intersect_output)
            intersect_cmd = subsample_cmd + " | " + intersect_cmd

        # Case: Run bedtools intersect on full bam
        else:
            intersect_cmd = "%s intersect -a %s -b %s -c -sorted -bed -g %s > %s !LOG2!" \
                            % (bedtools, bam, target_bed, genome_file, intersect_output)

        intersect_cmd = "%s%s ; %s" % (sort_bam_cmd, make_genome_file_cmd, intersect_cmd)

        # Set name of output file
        self.output = dict()
        self.output["capture_bed"] = intersect_output

        return intersect_cmd

