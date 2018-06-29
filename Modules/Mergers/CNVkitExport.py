from Modules import Merger

class CNVkitExport(Merger):
    def __init__(self, module_id, is_docker = False):
        super(CNVkitExport, self).__init__(module_id, is_docker)
        self.output_keys    = ["export"]

    def define_input(self):
        self.add_argument("cns",            is_required=True)
        self.add_argument("cnvkit",         is_required=True, is_resource=True)
        self.add_argument("export_type",    is_required=True, default_value="seg")
        self.add_argument("nr_cpus",        is_required=True, default_value=8)
        self.add_argument("mem",            is_required=True, default_value="nr_cpus * 2")

    def define_output(self):

        #generate unique name for reference bin file generated by CNVKit
        export_type = self.get_argument("export_type")

        if export_type == "seg":
            export_file_name = self.generate_unique_file_name(extension=".seg.txt")
        elif export_type == "bed":
            export_file_name = self.generate_unique_file_name(extension=".cnv.bed")
        elif export_type == "vcf":
            export_file_name = self.generate_unique_file_name(extension=".cnv.vcf")
        elif export_type == "cdt":
            export_file_name = self.generate_unique_file_name(extension=".cnv.cdt")
        elif export_type == "jtv":
            export_file_name = self.generate_unique_file_name(extension=".cnv.jtv.txt")
        elif export_type == "theta":
            export_file_name = self.generate_unique_file_name(extension=".theta2.interval_count")
        else:
            raise NotImplementedError("Export method {0} is not supported in CNVKit".format(export_type))

        self.add_output("export", export_file_name)

    def define_command(self):

        # Get arguments
        cns         = self.get_argument("cns")
        cnvkit      = self.get_argument("cnvkit")
        export_type = self.get_argument("export_type")

        #get the filename which store segmentation values
        export_file_name = self.get_output("export")

        #join cns file names with space delimiter
        cns = " ".join(cns)

        #command line to export CNVkit CNS to GISTIC2 seg format
        cmd = "{0} export {1} {2} -o {3} !LOG3!".format(cnvkit, export_type, cns, export_file_name)

        return cmd