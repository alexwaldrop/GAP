import logging

from GAP_interfaces import Tool

__main_class__ = "SamtoolsDepth"

class SamtoolsDepth(Tool):

    def __init__(self, config, sample_data):
        super(SamtoolsDepth, self).__init__()

        self.config         = config
        self.sample_data    = sample_data

        # Path to samtools
        self.samtools       = self.config["paths"]["tools"]["samtools"]

        # Path to bedtools
        self.bedtools       = self.config["paths"]["tools"]["bedtools"]

        # Module is splittable by chromosome
        self.can_split      = True
        self.splitter       = "SamtoolsDepthSplitter"
        self.merger         = "SamtoolsDepthMerge"

        self.nr_cpus        = 1
        self.mem            = 3

        # Bam alignment
        self.bam            = None

        # Bed file containing target regions of interest
        self.target_bed     = config["paths"]["resources"]["target_bed"]

        # Define input/output keys
        self.input_keys             = ["bam"]
        self.splitted_input_keys    = ["bam", "location"]
        self.output_keys            = ["samtools_depth"]
        self.splitted_output_keys   = ["samtools_depth"]

    def get_command(self, **kwargs):

        # Obtaining the arguments
        self.bam        = kwargs.get("bam",         None)
        self.chrm       = kwargs.get("location",    None)
        self.split_id   = kwargs.get("split_id",    None)
        self.target_bed = kwargs.get("target_bed",  self.config["paths"]["resources"]["target_bed"])

        bam_prefix = self.bam.split(".")[0]

        #generate names of output files
        if self.split_id is None:
            #case: no splitting
            output_bam = "%s.depth.txt" % bam_prefix

            if self.target_bed is None:
                #case: do not subset by target bedfile
                final_cmd = "%s depth -a %s > %s !LOG2!" % (self.samtools, self.bam, output_bam)
            else:
                #case: subset samtools depth output by target bedfile
                #samtools depth command
                depth_cmd = "%s depth -a %s" % (self.samtools, self.bam)

                #append commands for subsetting to target depth
                final_cmd = depth_cmd + " | " + self.get_samtools_depth_bed_command(output_bam, self.target_bed)
        else:
            #case: split by chromosome
            output_bam = "%s.%d.depth.txt" % (bam_prefix, self.split_id)

            if self.target_bed is None:
                #case: get depth for all positions (WGS)
                final_cmd = "%s depth -r %s -a %s > %s !LOG2!" % (self.samtools, self.chrm, self.bam, output_bam)

            else:
                #case: get depth at positions specified by target bedfile (Exome, TargetCapture)
                depth_cmd = "%s depth -r %s -a %s !LOG2!" % (self.samtools, self.chrm, self.bam)
                final_cmd = depth_cmd + " | " + self.get_samtools_depth_bed_command(output_bam, self.target_bed)

        # Set name of output file
        self.output = dict()
        self.output["samtools_depth"] = output_bam

        return final_cmd

    def get_samtools_depth_bed_command(self, output_bam, target_bed):
        #returns command for subsetting samtools depth output based on a target bed file

        # convert depth output to bedfile
        depth_2_bed_cmd = "awk 'BEGIN{OFS=\"\\t\"}{print $1,$2,$2+1,$3}' !LOG2!"

        # intersect depth file with target region bed file
        intersect_bed_cmd = "%s intersect -a %s -b stdin -sorted -wb !LOG2!" % (self.bedtools, target_bed)

        # convert output bed to samtools depth output
        bed_2_depth_cmd = "cut -f 4,5,7 > %s !LOG2!" % (output_bam)

        # chain subcommands with pipes for final command
        return depth_2_bed_cmd + " | " + intersect_bed_cmd + " | " + bed_2_depth_cmd