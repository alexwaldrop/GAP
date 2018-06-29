from Modules import Module

class Gistic2(Module):
    def __init__(self, module_id, is_docker = False):
        super(Gistic2, self).__init__(module_id, is_docker)
        self.output_keys = ["gistic_output_dir"]

    def define_input(self):
        self.add_argument("sample_name",        is_required=True)
        self.add_argument("gistic2",            is_required=True, is_resource=True)
        self.add_argument("analysis_type",      is_required=True)
        self.add_argument("export",             is_required=True)
        self.add_argument("refgene_mat",        is_required=True, is_resource=True)
        self.add_argument("genegistic",         is_required=True, default_value=1)
        self.add_argument("broad",              is_required=True, default_value=1)
        self.add_argument("brlen",              is_required=True, default_value=0.75)
        self.add_argument("conf",               is_required=True, default_value=0.75)
        self.add_argument("armpeel",            is_required=True, default_value=1)
        self.add_argument("savegene",           is_required=True, default_value=1)
        self.add_argument("gcm",                is_required=True, default_value="extreme")
        self.add_argument("ta",                 is_required=True, default_value=0.1)
        self.add_argument("td",                 is_required=True, default_value=0.1)
        self.add_argument("twosides",           is_required=True, default_value=1)
        self.add_argument("verbose",            is_required=True, default_value=30)
        self.add_argument("nr_cpus",            is_required=True, default_value=8)
        self.add_argument("mem",                is_required=True, default_value="nr_cpus * 2")

    def define_output(self):

        #get the anlysis type
        analysis_type = self.get_argument("analysis_type")

        if analysis_type == "tumor":
            self.add_output("gistic_output_dir", "gistic_tumors_only")
        elif analysis_type == "normal":
            self.add_output("gistic_output_dir", "gistic_normals_only")
        else:
            raise NotImplementedError("Analysis type {0} is not implemented yet.".format(analysis_type))

    def define_command(self):

        # Get arguments
        gistic2         = self.get_argument("gistic2")
        export          = self.get_argument("export")
        refgene_mat     = self.get_argument("refgene_mat")
        genegisitc      = self.get_argument("genegistic")
        broad           = self.get_argument("broad")
        brlen           = self.get_argument("brlen")
        conf            = self.get_argument("conf")
        armpeel         = self.get_argument("armpeel")
        savegene        = self.get_argument("savegene")
        gcm             = self.get_argument("gcm")
        ta              = self.get_argument("ta")
        td              = self.get_argument("td")
        twosides        = self.get_argument("twosides")
        verbose         = self.get_argument("verbose")

        #get the output directory name to store GISTIC2 results
        gistic_output_dir = self.get_output("gistic_output_dir")

        #make base directories to store GISTIC2 results
        mkdir_cmd = "mkdir -p {0}".format(gistic_output_dir)

        # Command line for GISTIC2 run
        cmd = "{15} ; {0} -b {1} -seg {2} -refgene {3} -genegistic {4} -broad {5} -brlen {6} " \
              "-conf {7} -armpeel {8} -savegene {9} -gcm {10} -ta {11} -td {12} -twosides {13} " \
              "-v {14} !LOG3!".format(gistic2, gistic_output_dir, export, refgene_mat, genegisitc, broad, brlen, conf,
                                      armpeel, savegene, gcm, ta, td, twosides, verbose, mkdir_cmd)

        return cmd