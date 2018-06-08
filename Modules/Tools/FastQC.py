from Modules import Module

class FastQC(Module):
    def __init__(self, module_id):
        super(FastQC, self).__init__(module_id)
        self.output_keys = ["R1_fastqc", "R2_fastqc"]

    def define_input(self):
        self.add_argument("R1",         is_required=True)
        self.add_argument("R2")
        self.add_argument("fastqc",     is_required=True, is_resource=True)
        self.add_argument("java",       is_required=True, is_resource=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=2)
        self.add_argument("mem",        is_required=True, default_value=5)

    def define_output(self):

        # Declare R1 fastqc output filename
        r1 = self.get_argument("R1")
        r1_out = "%s_fastqc" % r1.replace(".fastq.gz","").replace(".fastq","")
        self.add_output("R1_fastqc", r1_out)

        # Conditionally declare R2 fastqc output filename
        r2 = self.get_argument("R2")
        if r2 is not None:
            r2_out = "%s_fastqc" % r2.replace(".fastq.gz","").replace(".fastq","")
            self.add_output("R2_fastqc", r2_out)
        else:
            self.add_output("R2_fastqc", None, is_path=False)

    def define_command(self):
        # Generate command for running Fastqc
        fastqc  = self.get_argument("fastqc")
        java    = self.get_argument("java")
        r1      = self.get_argument("R1")
        r2      = self.get_argument("R2")
        nr_cpus = self.get_argument("nr_cpus")

        if r2 is not None:
            # Run Fastqc on R1 and R2
            cmd = "%s -t %d --java %s --nogroup --extract %s %s !LOG3!" % (fastqc, nr_cpus, java, r1, r2)
        else:
            # Run Fastqc on a single R1
            cmd = "%s -t %d --java %s --nogroup --extract %s !LOG3!" % (fastqc, nr_cpus, java, r1)
        return cmd
