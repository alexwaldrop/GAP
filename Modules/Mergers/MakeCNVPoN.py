from Modules import Merger

class MakeCNVPoN(Merger):
    def __init__(self, module_id, is_docker = False):
        super(MakeCNVPoN, self).__init__(module_id, is_docker)
        self.output_keys    = ["ref_cnn"]

    def define_input(self):
        self.add_argument("bam",            is_required=True)
        self.add_argument("bam_idx",        is_required=True)
        self.add_argument("cnvkit",         is_required=True, is_resource=True)
        self.add_argument("ref",            is_required=True, is_resource=True)
        self.add_argument("targets",        is_required=True, is_resource=True)
        self.add_argument("access",         is_required=True, is_resource=True)
        self.add_argument("method",         is_required=True, default_value="hybrid")
        self.add_argument("nr_cpus",        is_required=True, default_value=32)
        self.add_argument("mem",            is_required=True, default_value="nr_cpus * 2")

    def define_output(self):

        ref_cnn_file = self.generate_unique_file_name(extension=".ref.cnn")
        self.add_output("ref_cnn", ref_cnn_file)

    def define_command(self):

        # Get arguments
        bams        = self.get_argument("bam")
        cnvkit      = self.get_argument("cnvkit")
        ref         = self.get_argument("ref")
        targets     = self.get_argument("targets")
        access      = self.get_argument("access")
        method      = self.get_argument("method")
        nr_cpus     = self.get_argument("nr_cpus")

        #get the filename which store Panel of Normal ref cnn
        ref_cnn = self.get_output("ref_cnn")

        #join cns file names with space delimiter
        bams = " ".join(bams)

        # Get current working dir
        working_dir = self.get_output_dir()

        # generate command line for cnvkit for hybrid (WES) method
        if method == "hybrid":
            cmd = "{0} batch --normal {1} --targets {2} --fasta {3} --access {4} " \
                  "--output-reference {5} --output-dir {6} -p {7}".format(cnvkit, bams, targets, ref, access, ref_cnn,
                                                                   working_dir, nr_cpus)

        # generate command line for cnvkit for WGS method
        elif method == "wgs":
            cmd = "{0} batch --normal {1} --targets {2} --fasta {3} --access {4} " \
                  "--output-reference {5} --output-dir {6} -p {7} --method {8}".format(cnvkit, bams, targets, ref,
                                                                                       access, ref_cnn, working_dir,
                                                                                       nr_cpus, method)

        else:
            raise NotImplementedError("Method {0} is not implemented in CNVKit module.".format(method))

        cmd = "{0} !LOG3!".format(cmd)

        return cmd