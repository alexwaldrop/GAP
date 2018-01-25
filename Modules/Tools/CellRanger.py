import os

from Modules import Module

class CellRanger(Module):
    def __init__(self, module_id):
        super(CellRanger, self).__init__(module_id)

        self.input_keys = ["sample_name", "R1", "R2",
                           "cellranger", "singlecell_rnaseq_ref",
                           "nr_cpus", "mem"]

        self.output_keys = ["cellranger_output_dir"]

    def define_input(self):
        self.add_argument("sample_name",            is_required=True)
        self.add_argument("R1",                     is_required=True)
        self.add_argument("R2",                     is_required=True)
        self.add_argument("cellranger",             is_required=True, is_resource=True)
        self.add_argument("singlecell_rnaseq_ref",  is_required=True, is_resource=True)
        self.add_argument("nr_cpus",                is_required=True, default_value=12)
        self.add_argument("mem",                    is_required=True, default_value=48)

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
        transcriptome   = self.get_arguments("singlecell_rnaseq_ref").get_value()
        nr_cpus         = self.get_arguments("nr_cpus").get_value()
        mem             = self.get_arguments("mem").get_value()

        cellranger_dir  = os.path.dirname(cellranger)
        source_path     = os.path.join(cellranger_dir, "sourceme.bash")
        wrk_dir         = platform.get_workspace_dir()
        fastq_dir       = os.path.join(wrk_dir, "fastqs")

        # Make sure that the fastq directory is formatted like a directory
        if not fastq_dir.endswith("/"):
            fastq_dir += "/"

        # Coerce R1, R2 to lists even if they're single files
        # This means R1, R2 can be list of fastq files or a single fastq
        if not isinstance(R1, list):
            R1 = [R1]
            R2 = [R2]

        # We accommodate three idiosynchroses of Cell Ranger:
        # 1. CR accepts a folder of fastqs, not a list of files,
        #    so we move the fastqs to fastq_dir before calling Cell Ranger
        # 2. CR only accepts certain formats of fastq file names,
        #    so we rename the fastq files when moving
        mv_R1_cmd = ""
        mv_R2_cmd = ""
        for i in range(len(R1)):
            new_R1 = os.path.join(fastq_dir, "sample_S0_L000_R1_00{}.fastq.gz".format(i))
            new_R2 = os.path.join(fastq_dir, "sample_S0_L000_R2_00{}.fastq.gz".format(i))
            mv_R1_cmd += "mv {0} {1};".format(R1, new_R1)
            mv_R2_cmd += "mv {0} {1};".format(R2, new_R2)


        # 3. If interrupted, the lock file needs to be removed before restarting,
        #    so we remove the lock file just in case it exists
        cmd = "cd {0}; source {1}; rm -f {0}{5}/_lock; mkdir {2}; {3} {4} " \
              "cellranger count --id={5} --fastqs={2} --transcriptome={6} --localcores={7} --localmem={8}".format(
            wrk_dir, source_path, fastq_dir, mv_R1_cmd, mv_R2_cmd, sample_name,
            transcriptome, nr_cpus, mem)

        return cmd
