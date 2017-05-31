import logging
import math

from GAP_interfaces import Splitter

__main_class__ = "BwaFastqSplitter"

class BwaFastqSplitter(Splitter):
    def __init__(self, platform, tool_id, main_module_name=None):
        super(BwaFastqSplitter, self).__init__(platform, tool_id, main_module_name)

        self.nr_cpus = self.main_server_nr_cpus
        self.mem = self.main_server_mem

        self.input_keys = ["R1", "R2"]
        self.output_keys = ["R1", "R2", "nr_cpus"]

        self.req_tools = []
        self.req_resources = []

        # BWA-MEM aligning speed
        self.ALIGN_SPEED = 10 ** 8  # bps/vCPU for 10 mins of processing
        self.READ_LENGTH = 125 #TODO: Add function to get read length from file for
        self.MAX_NR_CPUS = self.max_nr_cpus

    def init_split_info(self, **kwargs):
        # Obtaining the arguments
        R1 = kwargs.get("R1", None)
        nr_cpus = kwargs.get("nr_cpus", self.nr_cpus)

        # Identifying the total number of reads
        logging.info("Counting the number of reads in the FASTQ files.")
        nr_reads = self.get_nr_reads(R1, nr_cpus)

        # Computing the number of lines to be split for each file considering:
        #  - The aligning speed which is in bps/vCPU
        #  - The maximum number of vCPUs alloed on the platform
        #  - The difference between read and read pair (divide by 2)
        nr_reads_per_split = self.ALIGN_SPEED / self.READ_LENGTH / 2 * self.MAX_NR_CPUS
        nr_splits = int(math.ceil(nr_reads * 1.0 / nr_reads_per_split))

        # Set number of lines per split to be access in get_command()
        self.nr_lines_per_split = nr_reads_per_split * 4

        # Create new dictionary for each split
        for i in range(nr_splits - 1):
            self.output.append({"nr_cpus": self.MAX_NR_CPUS,
                                "split_name": "%02d" % i,
                                "R1": None,
                                "R2": None})

        # Create final split using remaining CPUs
        # Determine number of CPUs available for last split
        nr_cpus_needed = int(math.ceil(nr_reads * self.READ_LENGTH * 2 * 1.0 / self.ALIGN_SPEED))
        nr_cpus_remaining = nr_cpus_needed % self.MAX_NR_CPUS
        nr_cpus_remaining += nr_cpus_remaining % 2
        self.output.append({"nr_cpus": nr_cpus_remaining,
                            "split_name": "%02d" % int(nr_splits - 1),
                            "R1": None,
                            "R2": None})

    def init_output_file_paths(self, **kwargs):
        for i in range(len(self.output)):
            split_name = self.output[i]["split_name"]
            self.generate_output_file_path("R1", "R1.fastq", split_id=i, split_name=split_name)
            self.generate_output_file_path("R2", "R2.fastq", split_id=i, split_name=split_name)

    def get_command(self, **kwargs):

        # Obtaining the arguments
        R1 = kwargs.get("R1", None)
        R2 = kwargs.get("R2", None)
        nr_cpus = kwargs.get("nr_cpus", self.nr_cpus)

        # Get output file prefix
        output_prefix = self.output[0]["R1"].split(".%s." % self.output[0]["split_name"])[0] + "."

        # Generate command for splitting R1
        split_r1_cmd = self.get_unix_split_cmd(R1, nr_cpus, output_prefix, output_suffix=".R1.fastq")

        # Generate command for splitting R2
        split_r2_cmd = self.get_unix_split_cmd(R2, nr_cpus, output_prefix, output_suffix=".R2.fastq")

        return "%s !LOG2! && %s !LOG2!" % (split_r1_cmd, split_r2_cmd)

    def get_nr_reads(self, R1, nr_cpus):
        # Obtain the number of lines in the FASTQ
        if R1.endswith(".gz"):
            cmd = "pigz -p %d -d -k -c %s | wc -l" % (nr_cpus, R1)
        else:
            cmd = "cat %s | wc -l" % R1

        main_instance = self.platform.get_main_instance()
        main_instance.run_command("fastq_count", cmd, log=False)
        out, err = main_instance.get_proc_output("fastq_count")

        if err != "":
            err_msg = "Could not obtain the number of reads in the FASTQ file. "
            err_msg += "\nThe following command was run: \n  %s " % cmd
            err_msg += "\nThe following error appeared: \n  %s" % err
            logging.error(err_msg)
            exit(1)

        nr_reads = int(out) / 4

        return nr_reads

    def get_unix_split_cmd(self, fastq_file, nr_cpus, output_prefix, output_suffix):
        # Return command for using the unix 'split' command to split a fastq file into chunks
        # Automatically detects whether to decompress fastq file

        if fastq_file.endswith(".gz"):
            split_cmd = "pigz -p %d -d -k -c %s | split --suffix-length=2 --numeric-suffixes --additional-suffix=%s --lines=%d - %s" \
                     % (nr_cpus, fastq_file, output_suffix, self.nr_lines_per_split, output_prefix)
        else:
            split_cmd = "split --suffix-length=2 --numeric-suffixes --additional-suffix=%s --lines=%d %s %s" \
                     % (output_suffix, self.nr_lines_per_split, fastq_file, output_prefix)
        return split_cmd