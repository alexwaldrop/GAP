import os

from GAP_interfaces import Tool

__main_class__ = "BedtoolsCaptureEfficiency"

class BedtoolsCaptureEfficiency(Tool):

    def __init__(self, config, sample_data):
        super(BedtoolsCaptureEfficiency, self).__init__(config, sample_data)

        self.temp_dir       = self.config["paths"]["instance_tmp_dir"]

        self.can_split      = False

        self.nr_cpus        = self.config["platform"]["MS_nr_cpus"]
        self.mem            = self.config["platform"]["MS_mem"]

        # I/O keys
        self.input_keys     = ["bam"]
        self.output_keys    = ["capture_bed"]

        # Required tools and resources
        self.req_tools      = ["bedtools", "samtools"]
        self.req_resources  = ["target_bed", "ref"]

        # Bam input file
        self.bam            = None

    def get_command(self, **kwargs):

        # Get command for running bedtools intersect to determine capture efficiency for a bam over a set of BED-formatted targets
        # Obtaining the arguments
        self.bam                = kwargs.get("bam",                 None)
        self.is_bam_sorted      = kwargs.get("is_bam_sorted",       True)
        self.subsample_perc     = kwargs.get("subsample_perc",      0.25)
        self.samtools           = kwargs.get("samtools",            self.tools["samtools"])
        self.bedtools           = kwargs.get("bedtools",            self.tools["bedtools"])
        self.target_bed         = kwargs.get("target_bed",          self.resources["target_bed"])
        self.ref                = kwargs.get("ref",                 self.resources["ref"])

        # Generate output filename
        bam_prefix = self.bam.split(".")[0]
        intersect_output = "%s.intersect.txt" % bam_prefix

        # Sort bam if necessary
        sort_bam_cmd = ""
        if not self.is_bam_sorted:
            # Get new filename for sorted bed
            bam_basename    = os.path.join(self.temp_dir, self.bam.split("/")[-1])
            sorted_bam      = "%s.sorted.%s" % (bam_basename.split(".")[0], ".".join(bam_basename.split(".")[1:]))
            # Sort and index bam and set self.bam to the name of the sorted bam
            sort_bam_cmd    = "%s sort %s > %s !LOG2!" % (self.samtools, self.bam, sorted_bam)
            index_bam_cmd   = "%s index %s !LOG2!" % (self.samtools, sorted_bam)
            sort_bam_cmd    = sort_bam_cmd + " ; " + index_bam_cmd + " ; "
            self.bam        = sorted_bam

        # Make genome file to specify sort order of chromosomes in BAM
        genome_file = "%s.genome" % bam_prefix
        make_genome_file_cmd = "%s idxstats %s | awk 'BEGIN{OFS=\"\\t\"}{print $1,$2}' > %s !LOG2!" % \
                               (self.tools["samtools"], self.bam, genome_file)

        # Case: Run bedtools intersect on subsampled bam
        if (self.subsample_perc < 1.0) and (self.subsample_perc > 0.0):
            # generate command for subsampling bam file
            subsample_cmd = "%s view -s %f -b %s" % (self.tools["samtools"], self.subsample_perc, self.bam)
            intersect_cmd = "%s intersect -a stdin -b %s -c -sorted -bed -g %s > %s !LOG2!" \
                            % (self.tools["bedtools"], self.target_bed, genome_file, intersect_output)
            intersect_cmd = subsample_cmd + " | " + intersect_cmd

        # Case: Run bedtools intersect on full bam
        else:
            intersect_cmd = "%s intersect -a %s -b %s -c -sorted -bed -g %s > %s !LOG2!" \
                            % (self.tools["bedtools"], self.bam, self.target_bed, genome_file, intersect_output)

        intersect_cmd = "%s%s ; %s" % (sort_bam_cmd, make_genome_file_cmd, intersect_cmd)

        # Set name of output file
        self.output = dict()
        self.output["capture_bed"] = intersect_output

        return intersect_cmd

