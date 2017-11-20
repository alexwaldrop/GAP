from Modules import Module
import os

class CellRanger(Module):
    def __init__(self, module_id):
        super(CellRanger, self).__init__(module_id)

        self.input_keys = ["sample_name", "fastq_folder", "cellranger",
                           "singlecell_hg19_rnaseq_ref", "nr_cpus", "mem"]

        self.output_keys = ["cellranger_output_dir"]

    def define_input(self):
        self.add_argument("sample_name",             is_required=True)
        self.add_argument("fastq_folder",   is_required=True)
        self.add_argument("cellranger",     is_required=True, is_resource=True)
        self.add_argument("singlecell_hg19_rnaseq_ref",
                          is_required=True, is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=12)
        self.add_argument("mem",            is_required=True, default_value=48)

    def define_output(self, platform, split_name=None):
        # Declare cell ranger output dir
        sample_name = self.get_arguments("sample_name").get_value()
        self.add_output(platform, "cellranger_output_dir", sample_name, is_path=True)

        # Declare web output
        #cell_ranger_dir = self.get_output("cellranger_output_dir")
        #self.add_output(platform, "web_report", os.path.join(cell_ranger_dir, "web"))

    def define_command(self, platform):
        # Generate command for running Cell Ranger
        cellranger      = self.get_arguments("cellranger").get_value()
        sample_name     = self.get_arguments("sample_name").get_value()
        fastq_dir       = self.get_arguments("fastq_folder").get_value()
        transcriptome   = self.get_arguments("singlecell_hg19_rnaseq_ref").get_value()
        nr_cpus         = self.get_arguments("nr_cpus").get_value()
        mem             = self.get_arguments("mem").get_value()

        cellranger_dir  = os.path.dirname(cellranger)
        source_path     = os.path.join(cellranger_dir, "sourceme.bash")
        wrk_dir         = platform.get_workspace_dir()

        # Make sure that the fastq directory is formatted like a directory
        if not fastq_dir.endswith("/"):
            fastq_dir += "/"

        cmd = "cd {0}; source {1}; cellranger count --id={2} --fastqs={3} --transcriptome={4} --localcores={5} --localmem={6}".format(
            wrk_dir, source_path, sample_name, fastq_dir, transcriptome, nr_cpus, mem)

        return cmd
