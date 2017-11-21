import os
from Modules import Module

class Star(Module):
    def __init__(self, module_id):
        super(Star, self).__init__(module_id)

        self.input_keys = ["R1", "R2", "star", "ref", "output_file_name_prefix", "quant_mode",
                           "out_unmapped_within_sam", "output_file_type", "twopass_mode", "nr_cpus", "mem"]

        self.output_keys = ["genome_mapped_bam", "transcriptome_mapped_bam", "gene_read_counts"]

        self.output_prefix = None

    def define_input(self):
        self.add_argument("R1",                         is_required=True)
        self.add_argument("R2")
        self.add_argument("star",                       is_required=True, is_resource=True)
        self.add_argument("ref",                        is_required=True, is_resource=True)
        self.add_argument("output_file_name_prefix",    default_value="STAR")
        self.add_argument("quant_mode",                 default_value="TranscriptomeSAM GeneCounts")
        self.add_argument("out_unmapped_within_sam",    default_value="Within")
        self.add_argument("output_file_type",           default_value="BAM SortedByCoordinate")
        self.add_argument("twopass_mode",               default_value="None")
        self.add_argument("nr_cpus",                    is_required=True, default_value=8)
        self.add_argument("mem",                        is_required=True, default_value=30)

    def define_output(self, platform, split_name=None):

        #get the prefix passed as argument
        file_extension = self.get_arguments("output_file_name_prefix").get_value()

        # Declare unique file name
        output_file_name = self.generate_unique_file_name(split_name=split_name,
                                                                 extension=file_extension)

        self.output_prefix = output_file_name

        self.add_output(platform, "genome_mapped_bam", "%s.Aligned.sortedByCoord.out.bam" % output_file_name)
        self.add_output(platform, "transcriptome_mapped_bam", "%s.Aligned.toTranscriptome.out.bam" % output_file_name)
        self.add_output(platform, "gene_read_counts", "%s.ReadsPerGene.out.tab" % output_file_name)

    def define_command(self, platform):

        # Get arguments to run STAR aligner
        R1                          = self.get_arguments("R1").get_value()
        R2                          = self.get_arguments("R2").get_value()
        star                        = self.get_arguments("star").get_value()
        ref                         = self.get_arguments("ref").get_value()
        quant_mod                   = self.get_arguments("quant_mode").get_value()
        out_unmapped_within_sam     = self.get_arguments("out_unmapped_within_sam").get_value()
        output_file_type            = self.get_arguments("output_file_type").get_value()
        twopass_mode                = self.get_arguments("twopass_mode").get_value()
        nr_cpus                     = self.get_arguments("nr_cpus").get_value()

        # Get current working dir
        working_dir = platform.get_workspace_dir()

        # Generate output file name prefix for STAR
        output_file_name_prefix = os.path.join(working_dir, "%s." % self.output_prefix)

        # Check the input FASTQ format
        if R1.endswith(".gz"):
            read_file_command = "zcat"
        else:
            read_file_command = "-"

        # Design command line based on read type (i.e. paired-end or single-end)
        if self.get_arguments("R2").get_value() is not None:
            cmd = "%s --runThreadN %s --genomeDir %s --readFilesIn %s %s --outFileNamePrefix %s --readFilesCommand %s \
                       --quantMode %s --outSAMunmapped %s --outSAMtype %s --twopassMode %s !LOG3!" % \
                        (star, nr_cpus, ref, R1, R2, output_file_name_prefix, read_file_command, quant_mod,
                         out_unmapped_within_sam, output_file_type, twopass_mode)
        else:
            cmd = "%s --runThreadN %s --genomeDir %s --readFilesIn %s --outFileNamePrefix %s --readFilesCommand %s \
                        --quantMode %s --outSAMunmapped %s --outSAMtype %s --twopassMode %s !LOG3!" % \
                       (star, nr_cpus, ref, R1, output_file_name_prefix, read_file_command, quant_mod,
                        out_unmapped_within_sam, output_file_type, twopass_mode)

        return cmd