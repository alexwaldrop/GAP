from Modules import Module

class QCReport2Table(Module):

    def __init__(self, module_id):
        super(QCReport2Table, self).__init__(module_id)

        self.input_keys     = ["qc_parser", "qc_report", "nr_cpus", "mem", "col_order", "alt_colnames"]
        self.output_keys    = ["qc_table"]

        # Command should be run on main processor
        self.quick_command = True

    def define_input(self):
        self.add_argument("qc_report",      is_required=True)
        self.add_argument("qc_parser",      is_required=True, is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=1)
        self.add_argument("mem",            is_required=True, default_value=1)
        self.add_argument("col_order",      is_required=True, default_value=None)
        self.add_argument("alt_colnames",   is_required=True, default_value=None)

    def define_output(self, platform, split_name=None):
        # Declare output summary filename
        summary_file = self.generate_unique_file_name(split_name=split_name, extension=".qc_report.table.txt")
        self.add_output(platform, "qc_table", summary_file)

    def define_command(self, platform):
        # Get options from kwargs
        input_file      = self.get_arguments("qc_report").get_value()
        qc_parser       = self.get_arguments("qc_parser").get_value()
        col_order       = self.get_arguments("col_order").get_value()
        alt_colnames    = self.get_arguments("alt_colnames").get_value()
        qc_table        = self.get_output("qc_table")

        # Create base command for PrintTable
        cmd = "%s PrintTable -i %s" % (qc_parser, input_file)

        # Add special arguments if necessary
        if col_order is not None:
            cmd += " --col-order %s" % " ".join(col_order)

        if alt_colnames is not None:
            cmd += " --alt-colnames %s" % " ".join(alt_colnames)

        # Direct output to output file
        cmd += " > %s !LOG2!" % qc_table
        return cmd