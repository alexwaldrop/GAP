from Module import Module

class Trimmomatic (Module):
    def __init__(self, module_id):
        super(Trimmomatic, self).__init__(module_id)

        self.input_keys = ["R1", "R2", "trimmomatic", "java", "adapters"]
        self.output_keys = ["R1", "R2", "R1_unpair", "R2_unpair"]

    def define_input(self):
        self.add_argument("R1", is_required=True)
        self.add_argument("R2")
        self.add_argument("trimmomatic", is_required=True, is_resource=True)
        self.add_argument("java", is_required=True, is_resource=True)
        self.add_argument("adapters", is_required=True, is_resource=True)
        self.add_argument("nr_cpus", is_required=True, default_value=2)
        self.add_argument("mem", is_required=True, default_value=5)

    def define_output(self, platform, split_name=None):

        self.generate_output_file_path(output_key="R1",
                                       extension="R1.trimmed.fastq")

        # Declare trimmed R1 output filename
        r1 = self.get_arguments("R1").get_value()
        r1_trimmed_prefix =

        # Declare discarded R1 output filename

        # Conditionally declare trimmed R2 output filename
        r2 = self.get_arguments("R2").get_value()

        # Conditionally declare discarded R2 output filename


        # Declare R1 fastqc output filename
        r1 = self.get_arguments("R1").get_value()
        r1_out = "%s_fastqc" % r1.replace(".fastq.gz","").replace(".fastq","")
        self.add_output("R1_fastqc", r1_out)

        # Conditionally declare R2 fastqc output filename
        r2 = self.get_arguments("R2").get_value()
        if r2 is not None:
            r2_out = "%s_fastqc" % r2.replace(".fastq.gz","").replace(".fastq","")
            self.add_output("R2_fastqc", r2_out)

    def define_command(self, platform):
        # Generate command for running Fastqc
        fastqc  = self.get_arguments("fastqc").get_value()
        java    = self.get_arguments("java").get_value()
        r1      = self.get_arguments("R1").get_value()
        r2      = self.get_arguments("R2").get_value()
        nr_cpus = self.get_arguments("nr_cpus").get_value()

        if r2 is not None:
            # Run Fastqc on R1 and R2
            cmd = "%s -t %d --java %s --nogroup --extract %s %s !LOG3!" % (fastqc, nr_cpus, java, r1, r2)
        else:
            # Run Fastqc on a single R1
            cmd = "%s -t %d --java %s --nogroup --extract %s !LOG3!" % (fastqc, nr_cpus, java, r1)
        return cmd