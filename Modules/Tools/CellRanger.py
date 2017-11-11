from Modules import Module

class CellRanger(Module):
    # TODO: make the module its own thing
    # TODO: run the module and debug it
    def __init__(self, module_id):
        super(CellRanger, self).__init__(module_id)

        self.input_keys = ["id", "fastq_folder", "cellranger",
                           "singlecell_hg19_rnaseq_ref", "nr_cpus", "mem"]
        self.output_keys = ["web_summary"]

        # Command should be run on main processor
        self.quick_command = True

    def define_input(self):
        self.add_argument("id",             is_required=True)
        self.add_argument("fastq_folder",   is_required=True)
        self.add_argument("cellranger",     is_required=True, is_resource=True)
        self.add_argument("singlecell_hg19_rnaseq_ref",
                          is_required=True, is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=2)
        self.add_argument("mem",            is_required=True, default_value=40)

    def define_output(self, platform, split_name=None):
        # Declare output summary filename
        summary_file = self.generate_unique_file_name(split_name=split_name, extension=".singlecell.websummary.txt")
        self.add_output(platform, "web_summary", summary_file)

    def define_command(self, platform):
        # Generate command for running Cell Ranger
        cellranger = self.get_arguments("cellranger").get_value()
        id = self.get_arguments("id").get_value()
        fastqs = self.get_arguments("fastq_folder").get_value()
        transcriptome = self.get_arguments("singlecell_hg19_rnaseq_ref").get_value()
        nr_cpus = self.get_arguments("nr_cpus").get_value()
        mem = self.get_arguments("mem").get_value()


        cmd = "{0} count --id={1} --fastqs={2} --transcriptome={3} --localcores={4} --localmem={5}".format(
            cellranger, id, fastqs, transcriptome, nr_cpus, mem)

        return cmd
