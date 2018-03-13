from Modules import Module

class Delly(Module):
    def __init__(self, module_id):
        super(Delly, self).__init__(module_id)

        self.input_keys = ["bam", "is_tumor", "ref", "exclude_list", "delly", "nr_cpus", "mem"]

        self.output_keys = ["bcf", "csi"]

        self.quick_command = True

    def define_input(self):
        self.add_argument("bam",            is_required=True)
        self.add_argument("delly",          is_required=True, is_resource=True)
        self.add_argument("is_tumor",       is_required=True)
        self.add_argument("ref",            is_required=True, is_resource=True)
        self.add_argument("exclude_list",   is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=2)
        self.add_argument("mem",            is_required=True, default_value=13)

    def define_output(self, platform, split_name=None):

        # Declare unique file name for bcf file
        bcf_file = self.generate_unique_file_name(split_name=split_name, extension=".bcf")
        self.add_output(platform, "bcf", bcf_file)

        # Declare the path to the csi index file
        csi_file = bcf_file + ".csi"
        self.add_output(platform, "csi", csi_file)

    def define_command(self, platform):

        # Get arguments to run Delly
        bam             = self.get_arguments("bam").get_value()
        is_tumor        = self.get_arguments("is_tumor").get_value()
        ref             = self.get_arguments("ref").get_value()
        exclude_list    = self.get_arguments("exclude_list").get_value()
        delly           = self.get_arguments("delly").get_value()

        # Get output paths
        bcf             = self.get_output("bcf")

        # Get libraries path
        libs_path = "{0}/modular-boost/stage/lib/:{0}/htslib/".format(delly.rsplit("/",1)[0])

        # Identify the tumor and the normal
        if is_tumor[0]:
            tumor = bam[0]
            normal = bam[1]
        else:
            tumor = bam[1]
            normal = bam[0]

        # Generate command
        if exclude_list:
            cmd = "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:%s ; %s call -x %s -g %s -o %s %s %s !LOG3!" \
                  % (libs_path, delly, exclude_list, ref, bcf, tumor, normal)
        else:
            cmd = "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:%s ; %s call -g %s -o %s %s %s !LOG3!" \
                  % (libs_path, delly, ref, bcf, tumor, normal)

        return cmd
