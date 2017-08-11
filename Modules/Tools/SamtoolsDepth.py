from Modules import Module

class SamtoolsDepth(Module):

    def __init__(self, module_id):
        super(SamtoolsDepth, self).__init__(module_id)

        # Define input/output keys
        self.input_keys     = ["bam", "bam_idx", "samtools", "bedtools", "nr_cpus", "mem"]
        self.output_keys    = ["samtools_depth"]

    def define_input(self):
        self.add_argument("bam",        is_required=True)
        self.add_argument("bam_idx",    is_required=True)
        self.add_argument("samtools",   is_required=True,   is_resource=True)
        self.add_argument("bedtools",   is_required=True,   is_resource=True)
        self.add_argument("target_bed", is_required=False,  is_resource=True)
        self.add_argument("nr_cpus",    is_required=True,   default_value=1)
        self.add_argument("mem",        is_required=True,   default_value=3)
        self.add_argument("location",   is_required=False)

    def define_output(self, platform, split_name=None):
        # Declare samtools depth out filename
        samtools_depth_out = self.generate_unique_file_name(split_name=split_name, extension=".samtools_depth.out")
        self.add_output(platform, "samtools_depth", samtools_depth_out)

        # Declare genome filename
        genome_file = self.generate_unique_file_name(split_name=split_name, extension=".bedtools.genome")
        self.add_output(platform, "genome_file", genome_file)

    def define_command(self, platform):

        # Get arguments for generating command
        bam                 = self.get_arguments("bam").get_value()
        samtools            = self.get_arguments("samtools").get_value()
        bedtools            = self.get_arguments("bedtools").get_value()
        chrm                = self.get_arguments("location").get_value()
        target_bed          = self.get_arguments("target_bed").get_value()

        # Get output filenames
        samtools_depth_out  = self.get_output("samtools_depth")
        genome_file         = self.get_output("genome_file")

        # Get base command for running samtools depth
        if chrm is None:
            depth_cmd = "%s depth -a %s" % (samtools, bam)
        else:
            depth_cmd = "%s depth -r %s -a %s" % (samtools, chrm, bam)

        # Append command for subsetting to a target region if necessary
        if target_bed is not None:
            # Subset samtools depth output to include only positions in target regions specified by bed file

            # Command to make genome file to specify sort order of chromosomes in BAM
            make_genome_file_cmd = "%s idxstats %s | awk 'BEGIN{OFS=\"\\t\"}{print $1,$2}' > %s !LOG2!" \
                                   % (samtools, bam, genome_file)

            # Command to subset results from samtools depth by a bed file
            subset_output_cmd = SamtoolsDepth.__get_subset_depth_bed_cmd(bedtools, target_bed)

            # Concatenate commands together
            cmd = "%s ; %s | %s > %s !LOG2!" \
                  % (make_genome_file_cmd, depth_cmd, subset_output_cmd, samtools_depth_out)

        else:
            # Return full samtools depth output
            cmd = " %s > %s !LOG2!" % (depth_cmd, samtools_depth_out)

        return cmd

    @staticmethod
    def __get_subset_depth_bed_cmd(bedtools, target_bed, genome_file):
        #returns command for subsetting samtools depth output based on a target bed file

        # convert depth output to bedfile
        depth_2_bed_cmd = "awk 'BEGIN{OFS=\"\\t\"}{print $1,$2,$2+1,$3}' !LOG2!"

        # intersect depth file with target region bed file
        intersect_bed_cmd = "%s intersect -a %s -b stdin -sorted -wb -g %s !LOG2!" \
                            % (bedtools, target_bed, genome_file)

        # convert output bed to samtools depth output
        bed_2_depth_cmd = "cut -f 4,5,7"

        # chain subcommands with pipes for final command
        return "%s | %s | %s" % (depth_2_bed_cmd, intersect_bed_cmd, bed_2_depth_cmd)
