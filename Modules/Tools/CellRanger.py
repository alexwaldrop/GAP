import os

from Modules import Module

class CellRanger(Module):
    def __init__(self, module_id):
        super(CellRanger, self).__init__(module_id)

        self.input_keys = ["sample_name", "R1", "R2",
                           "cellranger", "singlecell_hg19_rnaseq_ref",
                           "nr_cpus", "mem"]

        self.output_keys = ["cellranger_output_dir"]

    def define_input(self):
        self.add_argument("sample_name",                is_required=True)
        self.add_argument("R1",                         is_required=True)
        self.add_argument("R2",                         is_required=True)
        self.add_argument("cellranger",                 is_required=True, is_resource=True)
        self.add_argument("singlecell_hg19_rnaseq_ref", is_required=True, is_resource=True)
        self.add_argument("nr_cpus",                    is_required=True, default_value=12)
        self.add_argument("mem",                        is_required=True, default_value=48)

    def define_output(self, platform, split_name=None):
        # Declare cell ranger output dir
        sample_name = self.get_arguments("sample_name").get_value()
        self.add_output(platform, "cellranger_output_dir", sample_name, is_path=True)

    def define_command(self, platform):
        # Generate command for running Cell Ranger
        cellranger      = self.get_arguments("cellranger").get_value()
        sample_name     = self.get_arguments("sample_name").get_value()
        R1              = self.get_arguments("R1").get_value()
        R2              = self.get_arguments("R2").get_value()
        transcriptome   = self.get_arguments("singlecell_hg19_rnaseq_ref").get_value()
        nr_cpus         = self.get_arguments("nr_cpus").get_value()
        mem             = self.get_arguments("mem").get_value()

        cellranger_dir  = os.path.dirname(cellranger)
        source_path     = os.path.join(cellranger_dir, "sourceme.bash")
        wrk_dir         = platform.get_workspace_dir()
        fastq_dir       = os.path.join(wrk_dir, "fastqs")
        new_R1          = os.path.join(fastq_dir, "sample_S0_L000_R1_000.fastq.gz")
        new_R2          = os.path.join(fastq_dir, "sample_S0_L000_R2_000.fastq.gz")

        # Make sure that the fastq directory is formatted like a directory
        if not fastq_dir.endswith("/"):
            fastq_dir += "/"

        # We accommodate two idiosynchroses of Cell Ranger:
        # 1. it accepts a folder of fastqs, not a list of files
        # 2. it only accepts certain formats of fastq file names

        # Cell Ranger accepts a fastq folder rather than a list of fastqs,
        # so we move the fastqs to fastq_dir before calling Cell Ranger
        cmd = "cd {0}; source {1}; mkdir {2}; mv {3} {4}; mv {5} {6}; " \
              "cellranger count --id={7} --fastqs={2} --transcriptome={8} --localcores={9} --localmem={10}".format(
            wrk_dir, source_path, fastq_dir, R1, new_R1, R2, new_R2, sample_name, transcriptome, nr_cpus, mem)

        return cmd
