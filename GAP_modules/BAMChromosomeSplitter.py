__main_class__ = "BAMChromosomeSplitter"

class BAMChromosomeSplitter(object):

    def __init__(self, config, sample_data):
        self.config = config
        self.sample_data = sample_data

        self.samtools = self.config["paths"]["samtools"]

        self.temp_dir = self.config["general"]["temp_dir"]

        self.R1 = None
        self.R2 = None

        self.output_path = None
        self.pipeline_output_path = None

    def get_pipeline_output(self):
        return self.pipeline_output_path

    def get_output(self):
        return self.output_path

    def get_command(self, **kwargs):
        # Obtaining the arguments
        self.bam    = kwargs.get("bam",          self.sample_data["bam"])

        bam_prefix  = self.bam.split(".")[0]

        # Obtaining chromosome data from reference
        chrom_list = self.sample_data["chrom_list"]

        # Generating the commands
        cmds = list()

        # Obtaining the chromosomes in parallel
        cmd = '%s view -u -F 4 %s $chrom_name > %s_$chrom_name.bam' % (self.samtools, self.bam, bam_prefix)
        cmds.append('for chrom_name in %s; do %s & done' % (" ".join(chrom_list), cmd))

        # Obtaining the remaining chromosomes from the bam header
        chrom_regex = "\\b(%s)\\b" % ("|".join(chrom_list))
        cmds.append('remains=$(samtools view -H %s | grep "@SQ" | cut -f2 | tr -d "SN:" | egrep -iv "%s")' % (self.bam, chrom_regex))
        cmds.append('%s view -u -F 4 %s $remains > %s_remains.bam' % (self.samtools, self.bam, bam_prefix))

        # Obtaining the unaligned reads
        cmds.append('%s view -u -f 4 %s > %s_unmaped.bam' % (self.samtools, self.bam, bam_prefix))

        # Setting up the output paths
        self.output_path = [ {"bam": "%s_%s.bam" % (bam_prefix, chrom_name), "is_aligned":True} for chrom_name in chrom_list]
        self.output_path.append({"bam": "%s_remains.bam" % bam_prefix, "is_aligned":True})
        self.output_path.append({"bam": "%s_unmaped.bam" % bam_prefix, "is_aligned":False})

        return " && ".join(cmds)
