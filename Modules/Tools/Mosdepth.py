from Modules import Module

class Mosdepth(Module):
    def __init__(self, module_id, is_docker=False):
        super(Mosdepth, self).__init__(module_id, is_docker)
        self.output_keys    = ["mosdepth_dist"]

    def define_input(self):
        self.add_argument("bam",        is_required=True)
        self.add_argument("bam_idx",    is_required=True)
        self.add_argument("mosdepth",   is_resource=True,   is_required=True)
        self.add_argument("nr_cpus",    is_required=True,   default_value=6)
        self.add_argument("mem",        is_required=True,   default_value=12)
        self.add_argument("bed",        is_resource=True,   is_required=False)

    def define_output(self):

        # Determine if output name based on whether it's being subset by BED
        target_bed  = self.get_argument("bed")
        extension = ".mosdepth.global.dist.txt" if target_bed is None else ".mosdepth.region.dist.txt"


        # Mosdepth depth distribution summary
        dist_out = self.generate_unique_file_name(extension=extension)
        self.add_output("mosdepth_dist", dist_out)

    def define_command(self):
        # Get arguments for generating command
        bam             = self.get_argument("bam")
        mosdepth        = self.get_argument("mosdepth")
        nr_cpus         = self.get_argument("nr_cpus")
        target_bed      = self.get_argument("bed")
        mosdepth_out    = self.get_output("mosdepth_dist")

        # Get output prefix
        out_prefix = mosdepth_out.split(".mosdepth.")[0]

        # Generate mosdepth command with subset bed
        if target_bed is not None:
            return "{0} -t {1} -n --by {2} {3} {4} !LOG2!".format(mosdepth, nr_cpus, target_bed, out_prefix, bam)

        # Generate mosdepth command without subset bed
        return "{0} -t {1} -n {2} {3} !LOG2!".format(mosdepth, nr_cpus, out_prefix, bam)
