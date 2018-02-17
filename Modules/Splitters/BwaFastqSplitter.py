import logging
import math

from Modules import Module

class BwaFastqSplitter(Module):

    def __init__(self, module_id):
        super(BwaFastqSplitter, self).__init__(module_id)

        self.input_keys = ["R1", "R2", "nr_cpus", "mem"]
        self.output_keys = ["R1", "R2", "nr_cpus"]

        # BWA-MEM aligning speed constant
        self.ALIGN_SPEED = 10 ** 8  # bps/vCPU for 10 mins of processing

        self.quick_command = True

    def define_input(self):
        self.add_argument("R1",             is_required=True)
        self.add_argument("R2",             is_required=False)
        self.add_argument("nr_cpus",        is_required=True,   default_value=4)
        self.add_argument("mem",            is_required=True,   default_value="nr_cpus * 2")

    def define_output(self, platform, spilt_name=None):
        # Obtaining the arguments
        R1          = self.get_arguments("R1").get_value()
        R2          = self.get_arguments("R2").get_value()
        nr_cpus     = self.get_arguments("nr_cpus").get_value()
        max_nr_cpus = max(platform.get_max_nr_cpus(), 4)

        # Identifying the total number of reads
        try:
            logging.info("Counting the number of reads in the FASTQ files.")
            nr_reads = self.__get_nr_reads(platform, R1, nr_cpus)
        except:
            logging.error("Unable to determine number of reads in FASTQ: %s" % R1)
            raise

        # Computing the average read length
        try:
            logging.info("Computing average read length in the FASTQ files.")
            read_len = self.__get_read_len(platform, R1, nr_reads)
            logging.info("Average read length: %d" % read_len)
        except:
            logging.error("Unable to determine average read length in FASTQ: %s" % R1)
            raise

        # Computing the number of lines to be split for each file considering:
        #  - The aligning speed which is in bps/vCPU
        #  - The maximum number of vCPUs alloed on the platform
        #  - The difference between read and read pair (divide by 2)
        nr_reads_per_split  = self.ALIGN_SPEED / read_len / 2 * max_nr_cpus
        nr_splits           = int(math.ceil(nr_reads * 1.0 / nr_reads_per_split))

        # Set number of lines per split to be access in get_command()
        self.nr_lines_per_split = nr_reads_per_split * 4

        # Get name of working directory where files will be output
        wrk_dir = platform.get_workspace_dir()

        # Create new dictionary for each split
        for i in range(nr_splits - 1):
            # Generate filenames with split names as they'll appear after being generated with unix split function
            split_name = "%02d" % i
            r1_split = self.generate_unique_file_name(split_name=split_name, extension="R1.fastq", containing_dir=wrk_dir)
            r2_split = self.generate_unique_file_name(split_name=split_name, extension="R2.fastq", containing_dir=wrk_dir) if R2 is not None else None

            # Create next split
            split_data = {"nr_cpus" : max_nr_cpus,
                          "R1"      : r1_split,
                          "R2"      : r2_split}
            self.add_output(platform, split_name, split_data, is_path=False)

        # Create final split using remaining CPUs
        # Determine number of CPUs available for last split
        nr_cpus_needed      = int(math.ceil(nr_reads * read_len * 2 * 1.0 / self.ALIGN_SPEED))
        nr_cpus_remaining   = nr_cpus_needed % max_nr_cpus if nr_cpus_needed % max_nr_cpus else max_nr_cpus
        nr_cpus_remaining   += nr_cpus_remaining % 2
        nr_cpus_remaining   = max(nr_cpus_remaining, 4)

        # Make final split
        split_name  = "%02d" % int(nr_splits - 1)
        r1_split    = self.generate_unique_file_name(split_name=split_name, extension="R1.fastq", containing_dir=wrk_dir)
        r2_split    = self.generate_unique_file_name(split_name=split_name, extension="R2.fastq", containing_dir=wrk_dir) if R2 is not None else None

        # Create final split object
        split_data  = {"nr_cpus"    : nr_cpus_remaining,
                      "R1"          : r1_split,
                      "R2"          : r2_split}
        self.add_output(platform, split_name, split_data, is_path=False)

    def define_command(self, platform):

        # Obtaining the arguments
        # Obtaining the arguments
        R1          = self.get_arguments("R1").get_value()
        R2          = self.get_arguments("R2").get_value()
        nr_cpus     = self.get_arguments("nr_cpus").get_value()

        # Get output file prefix
        # Get output file basename
        split_name = self.output.keys()[0]
        output_basename = self.output[split_name]["R1"].split(split_name)[0]

        # Generate command for splitting R1
        split_r1_cmd = self.__get_unix_split_cmd(R1, nr_cpus, output_basename, output_suffix=".R1.fastq")

        if R2 is not None:
            # Generate command for splitting R2
            split_r2_cmd = self.__get_unix_split_cmd(R2, nr_cpus, output_basename, output_suffix=".R2.fastq")
            cmd = "%s !LOG2! && %s !LOG2!" % (split_r1_cmd, split_r2_cmd)
        else:
            cmd = "%s !LOG2!" % split_r1_cmd
        return cmd

    @staticmethod
    def __get_nr_reads(platform, R1, nr_cpus):
        # Obtain the number of lines in the FASTQ
        if R1.endswith(".gz"):
            cmd = "pigz -p %d -d -k -c %s | wc -l" % (nr_cpus, R1)
        else:
            cmd = "cat %s | wc -l" % R1
        out, err = platform.run_quick_command("fastq_count", cmd)
        nr_reads = int(out) / 4
        return nr_reads

    @staticmethod
    def __get_read_len(platform, R1, total_nr_reads):
        # Return average read length of first 100k reads

        # Figure out number of lines to examine (in case input contains fewer than 100K fastq entries)
        nr_reads    = total_nr_reads if total_nr_reads < 100000 else 100000
        num_lines   = nr_reads * 4

        # Output first 100k reads
        if R1.endswith(".gz"):
            head_cmd = "pigz -d -k -c %s | head -n %d" % (R1, num_lines)
        else:
            head_cmd = "cat %s | head -n %d" % (R1, num_lines)

        # Count number of total characters
        count_cmd   = "awk 'BEGIN{sum=0;}{if(NR%4==2){sum+=length($0);}}END{print sum;}'"
        cmd         = "%s | %s" % (head_cmd, count_cmd)
        out, err    = platform.run_quick_command("fastq_read_len", cmd)

        total_bases = float(out)
        return int(total_bases/nr_reads)

    def __get_unix_split_cmd(self, fastq_file, nr_cpus, output_prefix, output_suffix):
        # Return command for using the unix 'split' command to split a fastq file into chunks
        # Automatically detects whether to decompress fastq file

        if fastq_file.endswith(".gz"):
            split_cmd = "pigz -p %d -d -k -c %s | split --suffix-length=2 --numeric-suffixes --additional-suffix=%s --lines=%d - %s" \
                     % (nr_cpus, fastq_file, output_suffix, self.nr_lines_per_split, output_prefix)
        else:
            split_cmd = "split --suffix-length=2 --numeric-suffixes --additional-suffix=%s --lines=%d %s %s" \
                     % (output_suffix, self.nr_lines_per_split, fastq_file, output_prefix)
        return split_cmd
