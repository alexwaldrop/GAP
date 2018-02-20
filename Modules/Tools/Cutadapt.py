import logging

from Modules import Module

class Cutadapt (Module):
    def __init__(self, module_id):
        super(Cutadapt, self).__init__(module_id)

        self.input_keys     = ["R1", "fiveprime_adapter", "threeprime_adapter", "max_error_rate", "min_len", "max_len", "nr_cpus", "mem"]
        self.output_keys    = ["R1", "R1_unpair", "R1_untrimmed", "R1_toolong", "R1_tooshort"]
        self.quick_command  = True

    def define_input(self):
        self.add_argument("R1",         is_required=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=2)
        self.add_argument("mem",        is_required=True, default_value=8)

        # Optional cutadapt arguments
        self.add_argument("fiveprime_adapter",      is_required=True)
        self.add_argument("threeprime_adapter",     is_required=True)
        self.add_argument("max_error_rate",         is_required=True)
        self.add_argument("min_len",                is_required=True)
        self.add_argument("max_len",                is_required=True)

    def define_output(self, platform, split_name=None):

        # Declare trimmed R1 output filename
        r1_trimmed_ext  = ".R1.trimmed.fastq"
        r1_trimmed_out  = self.generate_unique_file_name(split_name=split_name, extension=r1_trimmed_ext)
        self.add_output(platform, "R1", r1_trimmed_out)

        # Declare discarded untrimmed R1 filename
        r1_untrimmed_out  = self.generate_unique_file_name(split_name=split_name, extension=".R1.untrimmed.fastq")
        self.add_output(platform, "R1_untrimmed", r1_untrimmed_out)

        # Declare discarded R1 too long filename
        r1_toolong_out = self.generate_unique_file_name(split_name=split_name, extension=".R1.toolong.fastq")
        self.add_output(platform, "R1_toolong", r1_toolong_out)

        # Declare discarded R1 too short filename
        r1_tooshort_out = self.generate_unique_file_name(split_name=split_name, extension=".R1.tooshort.fastq")
        self.add_output(platform, "R1_tooshort", r1_tooshort_out)

    def define_command(self, platform):
        # Generate command for running cutadapt

        # Get program options
        R1                  = self.get_arguments("R1").get_value()
        fiveprime_adapter   = self.get_arguments("fiveprime_adapter").get_value()
        threeprime_adapter  = self.get_arguments("threeprime_adapter").get_value()
        max_error_rate      = self.get_arguments("max_error_rate").get_value()
        min_len             = self.get_arguments("min_len").get_value()
        max_len             = self.get_arguments("max_len").get_value()

        # Get output filenames
        R1_out              = self.get_output("R1")
        R1_untrimmed_out    = self.get_output("R1_untrimmed")
        R1_toolong_out      = self.get_output("R1_toolong")
        R1_tooshort_out     = self.get_output("R1_tooshort")

        # Generate command for single-end trimmomatic
        cmd = "sudo pip install cutadapt ; "
        cmd += "cutadapt -g ^%s -a %s -e %s %s -n 2 -m %s -M %s --too-short-output %s --too-long-output %s --untrimmed-output %s > %s" % (fiveprime_adapter, threeprime_adapter,
                                                                                                                                          max_error_rate,
                                                                                                                                          R1, min_len,
                                                                                                                                          max_len, R1_tooshort_out,
                                                                                                                                          R1_toolong_out, R1_untrimmed_out,
                                                                                                                                          R1_out)
        return cmd