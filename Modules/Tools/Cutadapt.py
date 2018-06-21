import logging

from Modules import Module

class Cutadapt (Module):
    def __init__(self, module_id, is_docker = False):
        super(Cutadapt, self).__init__(module_id, is_docker)
        self.output_keys    = ["R1", "R1_unpair", "R1_untrimmed", "R1_toolong", "R1_tooshort"]

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

    def define_output(self):

        # Declare trimmed R1 output filename
        r1_trimmed_ext  = ".R1.trimmed.fastq"
        r1_trimmed_out  = self.generate_unique_file_name(extension=r1_trimmed_ext)
        self.add_output("R1", r1_trimmed_out)

        # Declare discarded untrimmed R1 filename
        r1_untrimmed_out  = self.generate_unique_file_name(extension=".R1.untrimmed.fastq")
        self.add_output("R1_untrimmed", r1_untrimmed_out)

        # Declare discarded R1 too long filename
        r1_toolong_out = self.generate_unique_file_name(extension=".R1.toolong.fastq")
        self.add_output("R1_toolong", r1_toolong_out)

        # Declare discarded R1 too short filename
        r1_tooshort_out = self.generate_unique_file_name(extension=".R1.tooshort.fastq")
        self.add_output("R1_tooshort", r1_tooshort_out)

    def define_command(self):

        # Get program options
        R1                  = self.get_argument("R1")
        fiveprime_adapter   = self.get_argument("fiveprime_adapter")
        threeprime_adapter  = self.get_argument("threeprime_adapter")
        max_error_rate      = self.get_argument("max_error_rate")
        min_len             = self.get_argument("min_len")
        max_len             = self.get_argument("max_len")

        # Get output filenames
        R1_out              = self.get_output("R1")
        R1_untrimmed_out    = self.get_output("R1_untrimmed")
        R1_toolong_out      = self.get_output("R1_toolong")
        R1_tooshort_out     = self.get_output("R1_tooshort")

        if not self.is_docker:

            # Generate command for single-end trimmomatic
            cmd = "sudo pip install cutadapt ; "
            cmd += "cutadapt -g ^{0} -a {1} -e {2} {3} -n 2 -m {4} -M {5} --too-short-output {6} --too-long-output {7} " \
                   "--untrimmed-output {8} > {9}".format(fiveprime_adapter, threeprime_adapter, max_error_rate, R1,
                                                         min_len, max_len, R1_tooshort_out, R1_toolong_out, R1_untrimmed_out,
                                                         R1_out)

        else:

            cmd = "cutadapt -g ^{0} -a {1} -e {2} {3} -n 2 -m {4} -M {5} --too-short-output {6} --too-long-output {7} " \
                   "--untrimmed-output {8} > {9}".format(fiveprime_adapter, threeprime_adapter, max_error_rate, R1,
                                                         min_len, max_len, R1_tooshort_out, R1_toolong_out,
                                                         R1_untrimmed_out,
                                                         R1_out)

        return cmd