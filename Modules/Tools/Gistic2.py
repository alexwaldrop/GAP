import os
from Modules import Module

class Gistic2(Module):
    def __init__(self, module_id):
        super(Gistic2, self).__init__(module_id)

        # self.input_keys = ["sample_name", "gistic2", "analysis_type", "seg", "base_dir", "refgene_mat", "genegistic", "broad",
        #                    "brlen", "conf", "armpeel", "savegene", "gcm", "ta", "td", "twosides", "verbose", "nr_cpus", "mem"]
        self.input_keys = ["sample_name", "gistic2", "analysis_type", "seg", "refgene_mat", "genegistic","broad",
                           "brlen", "conf", "armpeel", "savegene", "gcm", "ta", "td", "twosides", "verbose", "nr_cpus", "mem"]

        self.output_keys = ["gistic_output_dir"]

        # Command should be run on main processor
        self.quick_command = True

    def define_input(self):
        self.add_argument("sample_name",        is_required=True)
        self.add_argument("gistic2",            is_required=True, is_resource=True)
        self.add_argument("analysis_type",      is_required=True)
        self.add_argument("seg",                is_required=True)
        # self.add_argument("base_dir",           is_required=True, default_value="gistic_results")
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

    def define_output(self, platform, split_name=None):

        #get the anlysis type
        analysis_type = self.get_arguments("analysis_type").get_value()

        if analysis_type == "tumor":
            self.add_output(platform, "gistic_output_dir", "gistic_tumors_only")
        elif analysis_type == "normal":
            self.add_output(platform, "gistic_output_dir", "gistic_normals_only")
        else:
            raise NotImplementedError("Analysis type {0} is not implemented yet.".format(analysis_type))

    def define_command(self, platform):

        # Get arguments
        gistic2         = self.get_arguments("gistic2").get_value()
        seg             = self.get_arguments("seg").get_value()
        refgene_mat     = self.get_arguments("refgene_mat").get_value()
        genegisitc      = self.get_arguments("genegistic").get_value()
        broad           = self.get_arguments("broad").get_value()
        brlen           = self.get_arguments("brlen").get_value()
        conf            = self.get_arguments("conf").get_value()
        armpeel         = self.get_arguments("armpeel").get_value()
        savegene        = self.get_arguments("savegene").get_value()
        gcm             = self.get_arguments("gcm").get_value()
        ta              = self.get_arguments("ta").get_value()
        td              = self.get_arguments("td").get_value()
        twosides        = self.get_arguments("twosides").get_value()
        verbose         = self.get_arguments("verbose").get_value()

        #get the output directory name to store GISTIC2 results
        gistic_output_dir = self.get_output("gistic_output_dir")

        #make base directories to store GISTIC2 results
        platform.run_quick_command(job_name="making_base_dir", cmd="mkdir -p {0}".format(gistic_output_dir))

        gistic2_loc_on_instance = os.path.dirname(gistic2)

        # Command line for GISTIC2 run
        cmd = "{0} {15} -b {1} -seg {2} -refgene {3} -genegistic {4} -broad {5} -brlen {6} " \
              "-conf {7} -armpeel {8} -savegene {9} -gcm {10} -ta {11} -td {12} -twosides {13} " \
              "-v {14} !LOG3!".format(gistic2, gistic_output_dir, seg, refgene_mat, genegisitc, broad, brlen, conf,
                                      armpeel, savegene, gcm, ta, td, twosides, verbose, gistic2_loc_on_instance)

        return cmd