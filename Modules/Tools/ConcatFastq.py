import logging

from Modules import Module

class ConcatFastq(Module):
    # Module designed to concatentate one or more R1, R2 files from the same sample
    # An example would be if you'd resequenced the same sample and wanted to used all sequence data as if it were a single FASTQ
    # If > 1 read pair: concat to a single read pair
    # If 1 read pair: return original file name without doing anything
    def __init__(self, module_id):
        super(ConcatFastq, self).__init__(module_id)

        self.input_keys     = ["R1", "R2"]
        self.output_keys    = ["R1", "R2"]

        # Command should be run on main processor
        self.quick_command = True

    def define_input(self):
        self.add_argument("R1",         is_required=True)
        self.add_argument("R2")
        self.add_argument("nr_cpus",    is_required=True, default_value=1)
        self.add_argument("mem",        is_required=True, default_value=1)

    def define_output(self, platform, split_name=None):

        # Declare R1 output name
        r1 = self.get_arguments("R1").get_value()
        if not isinstance(r1, list):
            # Just pass the filename as is if no concatenation required (num R1 = 1)
            self.add_output(platform, "R1", r1, is_path=False)
        else:
            # Concatenate R1 files to new output
            extension = ".R1.fastq.gz" if r1[0].endswith(".gz") else "concat.R1.fastq"
            self.add_output(platform, "R1", self.generate_unique_file_name(split_name=split_name,extension=extension))

        # Declare R2 output name
        r2 = self.get_arguments("R2").get_value()
        if not isinstance(r2, list):
            # Either R2 is single path or R2 is None
            self.add_output(platform, "R2", r2, is_path=False)
        else:
            extension = ".R2.fastq.gz" if r2[0].endswith(".gz") else "concat.R2.fastq"
            self.add_output(platform, "R2", self.generate_unique_file_name(split_name=split_name, extension=extension))

    def define_command(self, platform):
        # Generate command for running Fastqc
        r1      = self.get_arguments("R1").get_value()
        r2      = self.get_arguments("R2").get_value()
        r1_out  = self.get_output("R1")
        r2_out  = self.get_output("R2")

        # Check to make sure r1 and r2 contain same number of files
        self.__check_input(r1, r2)

        cmd = None
        if r1 != r1_out:
            # Concat R1 if necessary
            cmd = "cat %s > %s !LOG2!" % (" ".join(r1), r1_out)

        if r2 != r2_out:
            # Concat R2 if necessary
            r2_cmd = "cat %s > %s !LOG2!" % (" ".join(r2), r2_out)
            # Join in the background so they run at the same time
            cmd = "%s & %s ; wait" % (cmd, r2_cmd)

        return cmd

    def __check_input(self, r1, r2):
        # Make sure each contains same number of fastq files
        error = False
        multi_r1    = isinstance(r1, list)
        multi_r2    = isinstance(r2, list)
        single_r1   = isinstance(r1, basestring)
        single_r2   = isinstance(r2, basestring)
        if multi_r1:
            if multi_r2 and len(r1) != len(r2):
                # Multiple R1, R2 but not the same in each
                error = True
                logging.error("ConcatFastq error! Input must contain same number of R1(%d) and R2(%d) fastq files!" % (len(r1), len(r2)))
            elif single_r2:
                # Multiple R1 only one R2
                error = True
                logging.error("ConcatFastq error! Input must contain same number of R1(%d) and R2(1) fastq files!" % len(r1))
        elif multi_r2 and single_r1:
            # One R1 multiple R2
            error = True
            logging.error("ConcatFastq error! Input must contain same number of R1(1) and R2(%d) fastq files!" % len(r2))
        if error:
            raise RuntimeError("Incorrect input to ConcatFastq!")




