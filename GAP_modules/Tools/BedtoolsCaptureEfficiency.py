import logging

from GAP_interfaces import Tool

__main_class__ = "BedtoolsCaptureEfficiency"

class BedtoolsCaptureEfficiency(Tool):

    def __init__(self, config, sample_data):
        super(BedtoolsCaptureEfficiency, self).__init__()

        self.config         = config
        self.sample_data    = sample_data

        self.bedtools       = self.config["paths"]["tools"]["bedtools"]
        self.samtools       = self.config["paths"]["tools"]["samtools"]

        self.temp_dir       = self.config["paths"]["instance_tmp_dir"]

        self.can_split      = False

        self.nr_cpus        = self.config["platform"]["MS_nr_cpus"]
        self.mem            = self.config["platform"]["MS_mem"]

        # Bed file containing target regions of interest
        self.target_bed     = self.config["paths"]["resources"]["target_bed"]
        self.ref            = self.config["paths"]["ref"]

        # Percent of reads to use for capture efficiency statistics
        self.subsample_perc = 0.25 #self.config["general"]["capture_efficiency_subsample_perc"]

        # I/O keys
        self.input_keys     = ["bam"]
        self.output_keys    = ["capture_bed"]

        # Bam input file
        self.bam            = None


    def get_command(self, **kwargs):

        # Get command for running bedtools intersect to determine capture efficiency for a bam over a set of BED-formatted targets
        # Obtaining the arguments
        self.bam            = kwargs.get("bam",             None)
        self.target_bed     = kwargs.get("target_bed",      self.config["paths"]["resources"]["target_bed"])
        self.subsample_perc = kwargs.get("subsample_perc",  self.subsample_perc)

        # Generate output filename
        bam_prefix = self.bam.split(".")[0]
        intersect_output = "%s.intersect.txt" % bam_prefix

        # Get command to make genome file for sorting
        genome_file = "%s.genome" % bam_prefix
        make_genome_file_cmd = "%s idxstats %s | awk 'BEGIN{OFS=\"\\t\"}{print $1,$2}' > %s !LOG2!" % \
                               (self.samtools, self.bam, genome_file)

        # Case: Run bedtools intersect on subsampled bam
        if (self.subsample_perc < 1.0) and (self.subsample_perc > 0.0):
            # generate command for subsampling bam file
            subsample_cmd = "%s view -s %f -b %s" % (self.samtools, self.subsample_perc, self.bam)
            intersect_cmd = "%s intersect -a stdin -b %s -c -sorted -bed -g %s > %s !LOG2!" \
                            % (self.bedtools, self.target_bed, genome_file, intersect_output)
            intersect_cmd = subsample_cmd + " | " + intersect_cmd

        # Case: Run bedtools intersect on full bam
        else:
            intersect_cmd = "%s intersect -a %s -b %s -c -sorted -bed -g %s > %s !LOG2!" \
                            % (self.bedtools, self.bam, self.target_bed, genome_file, intersect_output)

        intersect_cmd = "%s ; %s" % (make_genome_file_cmd, intersect_cmd)

        # Set name of output file
        self.output = dict()
        self.output["capture_bed"] = intersect_output

        return intersect_cmd

