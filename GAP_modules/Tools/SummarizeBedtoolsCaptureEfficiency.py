from GAP_interfaces import Tool

__main_class__ = "SummarizeBedtoolsCaptureEfficiency"

class SummarizeBedtoolsCaptureEfficiency(Tool):

    def __init__(self, config, sample_data):
        super(SummarizeBedtoolsCaptureEfficiency, self).__init__(config, sample_data)

        self.can_split      = False

        self.nr_cpus        = 1
        self.mem            = self.main_server_mem

        self.input_keys     = ["capture_bed"]
        self.output_keys    = ["summary_file"]

        self.req_tools      = ["qc_parser"]
        self.req_resources  = []

    def get_command(self, **kwargs):

        # Get options from kwargs
        capture_bed         = kwargs.get("capture_bed",     None)
        target_type         = kwargs.get("target_type",     None)

        # Set name of output file
        output = "%s.capturesummary.txt" % capture_bed.split(".")[0]

        # Generating command to parse bedtools coverage output
        cmd = "%s capture -i %s" % (self.tools["qc_parser"], capture_bed)

        # Add option for name of target type
        if target_type is not None:
            cmd += " --targettype %s" % target_type

        # Write output to summary file
        cmd += " > %s !LOG2!" % output

        # Generating the output
        self.output = dict()
        self.output["summary_file"] = output

        return cmd