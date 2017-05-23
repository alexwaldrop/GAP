from GAP_interfaces import Tool

__main_class__ = "SamtoolsDepth"

class SamtoolsDepth(Tool):

    def __init__(self, config, sample_data, tool_id):
        super(SamtoolsDepth, self).__init__(config, sample_data, tool_id)

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

        # Output file name
        depth_out = self.output["samtools_depth"]

        # Command to run samtools depth to get coverage depth from BAM at each position in genome
        depth_cmd = "%s depth -a %s" % (samtools, bam) if split_id is None else "%s depth -r %s -a %s" %(samtools, chrm, bam)

        if target_bed is not None:
            # Subset samtools depth output to include only positions in target regions specified by bed file

            # Command to make genome file to specify sort order of chromosomes in BAM
            make_genome_file_cmd = "%s idxstats %s | awk 'BEGIN{OFS=\"\\t\"}{print $1,$2}' > %s !LOG2!" % \
                                   (samtools, bam, self.output["genome_file"])

            # Command to subset results from samtools depth by a bed file
            subset_output_cmd = self.subset_depth_bed_command(bedtools, target_bed)

            cmd = "%s ; %s | %s > %s !LOG2!" \
                  % (make_genome_file_cmd, depth_cmd, subset_output_cmd, self.output["samtools_depth"])

        else:
            # Return full samtools depth output
            cmd = " %s > %s !LOG2!" % (depth_cmd, self.output["samtools_depth"])

        return cmd

    def init_output_file_paths(self, **kwargs):
        split_id = kwargs.get("split_id", None)
        self.generate_output_file_path("samtools_depth", "samtoolsdepth.out", split_id=split_id)
        self.generate_output_file_path("genome_file", "bedtools.genome")

    def subset_depth_bed_command(self, bedtools, target_bed):
        #returns command for subsetting samtools depth output based on a target bed file

        # convert depth output to bedfile
        depth_2_bed_cmd = "awk 'BEGIN{OFS=\"\\t\"}{print $1,$2,$2+1,$3}' !LOG2!"

        # intersect depth file with target region bed file
        intersect_bed_cmd = "%s intersect -a %s -b stdin -sorted -wb -g %s !LOG2!" % (bedtools, target_bed, self.output["genome_file"])

        # convert output bed to samtools depth output
        bed_2_depth_cmd = "cut -f 4,5,7"

        # chain subcommands with pipes for final command
        return "%s | %s | %s" % (depth_2_bed_cmd, intersect_bed_cmd, bed_2_depth_cmd)