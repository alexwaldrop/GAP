from Modules import Module

class GATKPrintReads(Module):

    def __init__(self, module_id):
        super(GATKPrintReads, self).__init__(module_id)

        self.input_keys             = ["bam", "bam_idx", "BQSR_report", "bam_idx", "location",
                                       "excluded_location", "gatk", "java", "ref", "nr_cpus", "mem"]

        self.output_keys            = ["bam"]

    def define_input(self):
        self.add_argument("bam",                is_required=True)
        self.add_argument("bam_idx",            is_required=True)
        self.add_argument("BQSR_report",        is_required=True)
        self.add_argument("gatk",               is_required=True, is_resource=True)
        self.add_argument("java",               is_required=True, is_resource=True)
        self.add_argument("ref",                is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=2)
        self.add_argument("mem",                is_required=True, default_value="nr_cpus * 2.5")
        self.add_argument("location")
        self.add_argument("excluded_location")

    def define_output(self, platform, split_name=None):
        # Declare bam output filename
        bam = self.generate_unique_name(split_name=split_name, extension=".recalibrated.bam")
        self.add_output(platform, "bam", bam)

    def define_command(self, platform):
        # Obtaining the arguments
        bam     = self.get_arguments("bam").get_value()
        BQSR    = self.get_arguments("BQSR_report").get_value()
        gatk    = self.get_arguments("gatk").get_value()
        java    = self.get_arguments("java").get_value()
        ref     = self.get_arguments("ref").get_value()
        L       = self.get_arguments("location").get_value()
        XL      = self.get_arguments("excluded_location").get_value()
        nr_cpus = self.get_arguments("nr_cpus").get_value()
        mem     = self.get_arguments("mem").get_value()

        # Get output file
        output_bam = self.get_output("bam")

        # Generate JVM options
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem * 4 / 5, platform.get_workspace_dir("tmp"))

        # Generating the PrintReads caller options
        opts = list()
        opts.append("-I %s" % bam)
        opts.append("-o %s" % output_bam)
        opts.append("-nct %d" % nr_cpus)
        opts.append("-R %s" % ref)
        opts.append("-BQSR %s" % BQSR)

        # Limit the locations to be processed
        if L is not None:
            if isinstance(L, list):
                for included in L:
                    opts.append("-L \"%s\"" % included)
            else:
                opts.append("-L \"%s\"" % L)
        if XL is not None:
            if isinstance(XL, list):
                for excluded in XL:
                    opts.append("-XL \"%s\"" % excluded)
            else:
                opts.append("-XL \"%s\"" % XL)

        # Generating command for GATK PrintReads
        cmd = "%s %s -jar %s -T PrintReads %s !LOG3!" % (java,
                                                         jvm_options,
                                                         gatk,
                                                         " ".join(opts))
        return cmd
