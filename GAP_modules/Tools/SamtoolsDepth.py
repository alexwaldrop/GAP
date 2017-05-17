from GAP_interfaces import Tool

__main_class__ = "SamtoolsDepth"

class SamtoolsDepth(Tool):

    def __init__(self, config, sample_data):
        super(SamtoolsDepth, self).__init__(config, sample_data)

        # Module is splittable by chromosome
        self.can_split      = True
        self.splitter       = "SamtoolsDepthSplitter"
        self.merger         = "SamtoolsDepthMerge"

        self.nr_cpus        = 1
        self.mem            = 3

        # Define input/output keys
        self.input_keys             = ["bam"]
        self.splitted_input_keys    = ["bam", "location"]
        self.output_keys            = ["samtools_depth"]
        self.splitted_output_keys   = ["samtools_depth"]

        # Define required tools/resources
        self.req_tools = ["samtools", "bedtools"]
        self.req_resources = []

    def get_command(self, **kwargs):

        # Obtaining the arguments
        bam        = kwargs.get("bam",         None)
        chrm       = kwargs.get("location",    None)
        split_id   = kwargs.get("split_id",    None)
        samtools   = kwargs.get("samtools",    self.tools["samtools"])
        bedtools   = kwargs.get("bedtools",    self.tools["bedtools"])
        target_bed = kwargs.get("target_bed",  self.resources["target_bed"])

        bam_prefix = bam.split(".")[0]

        #generate names of output files
        if split_id is None:
            #case: no splitting
            output_bam = "%s.depth.txt" % bam_prefix

            if target_bed is None:
                #case: do not subset by target bedfile
                final_cmd = "%s depth -a %s > %s !LOG2!" % (samtools, bam, output_bam)
            else:
                #case: subset samtools depth output by target bedfile
                #samtools depth command
                depth_cmd = "%s depth -a %s" % (samtools, bam)

                #append commands for subsetting to target depth
                final_cmd = depth_cmd + " | " + self.get_samtools_depth_bed_command(output_bam, target_bed, bedtools)
        else:
            #case: split by chromosome
            output_bam = "%s.%d.depth.txt" % (bam_prefix, split_id)

            if target_bed is None:
                #case: get depth for all positions (WGS)
                final_cmd = "%s depth -r %s -a %s > %s !LOG2!" % (samtools, chrm, bam, output_bam)

            else:
                #case: get depth at positions specified by target bedfile (Exome, TargetCapture)
                depth_cmd = "%s depth -r %s -a %s !LOG2!" % (samtools, chrm, bam)
                final_cmd = depth_cmd + " | " + self.get_samtools_depth_bed_command(output_bam, target_bed, bedtools)

        # Set name of output file
        self.output = dict()
        self.output["samtools_depth"] = output_bam

        return final_cmd

    def get_samtools_depth_bed_command(self, output_bam, target_bed, bedtools):
        #returns command for subsetting samtools depth output based on a target bed file

        # convert depth output to bedfile
        depth_2_bed_cmd = "awk 'BEGIN{OFS=\"\\t\"}{print $1,$2,$2+1,$3}' !LOG2!"

        # intersect depth file with target region bed file
        intersect_bed_cmd = "%s intersect -a %s -b stdin -sorted -wb !LOG2!" % (bedtools, target_bed)

        # convert output bed to samtools depth output
        bed_2_depth_cmd = "cut -f 4,5,7 > %s !LOG2!" % (output_bam)

        # chain subcommands with pipes for final command
        return depth_2_bed_cmd + " | " + intersect_bed_cmd + " | " + bed_2_depth_cmd