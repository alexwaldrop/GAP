import logging

from GAP_interfaces import Tool

__main_class__= "BwaAligner"

class BwaAligner(Tool):
    
    def __init__(self, platform, tool_id):
        super(BwaAligner, self).__init__(platform, tool_id)

        self.can_split      = True
        self.splitter       = "BwaFastqSplitter"
        self.merger         = "SamtoolsBAMMerge"

        self.nr_cpus        = self.max_nr_cpus    # BWA MEM can use as many CPUs as possible
        self.mem            = max(10, self.nr_cpus)    # BWA MEM should not need more than 1 GB/CPU

        self.input_keys             = ["R1", "R2"]
        self.splitted_input_keys    = ["R1", "R2", "nr_cpus"]
        self.output_keys            = ["bam"]
        self.splitted_output_keys   = ["bam"]

        self.req_tools      = ["bwa", "samtools"]
        self.req_resources  = ["ref"]

        self.sample_name    = self.pipeline_data.get_sample_name()

    def get_rg_header(self, R1):

        # Obtain the read header
        cmd = "head -n 1 %s" % R1
        main_instance = self.platform.get_main_instance()
        main_instance.run_command("fastq_header", cmd, log=False)
        out, err = main_instance.get_proc_output("fastq_header")

        if err != "":
            err_msg = "Could not obtain information for BwaAligner. "
            err_msg += "\nThe following command was run: \n  %s" % cmd
            err_msg += "\nThe following error appeared: \n  %s" % err
            logging.error(err_msg)
            exit(1)

        # Generating the read group information
        fastq_header_data = out.lstrip("@").strip("\n").split(":")
        rg_id = ":".join(fastq_header_data[0:4])        # Read Group ID
        rg_pu = fastq_header_data[-1]                   # Read Group Platform Unit
        rg_sm = self.config["sample"]["sample_name"]         # Read Group Sample
        rg_lb = self.config["sample"]["lib_name"]            # Read Group Library ID
        rg_pl = self.config["sample"]["seq_platform"]        # Read Group Platform used

        read_group_header = "\\t".join(["@RG", "ID:%s" % rg_id, "PU:%s" % rg_pu,
                                                          "SM:%s" % rg_sm, "LB:%s" % rg_lb, "PL:%s" % rg_pl])

        # Generating the read group header
        return read_group_header

    def get_command(self, **kwargs):

        # Obtaining the arguments
        R1                 = kwargs.get("R1",              None)
        R2                 = kwargs.get("R2",              None)
        nr_cpus            = kwargs.get("nr_cpus",         self.nr_cpus)

        # Generating command for alignment
        aligner_cmd = "%s mem -M -R \"%s\" -t %d %s %s %s !LOG2!" % (self.tools["bwa"], self.get_rg_header(R1), nr_cpus, self.resources["ref"], R1, R2)

        # Generating command for converting SAM to BAM
        sam_to_bam_cmd  = "%s view -uS -@ %d - !LOG2!" % (self.tools["samtools"], nr_cpus)

        # Generating command for sorting BAM
        bam_sort_cmd = "%s sort -@ %d - -o %s !LOG3!" % (self.tools["samtools"], self.nr_cpus, self.output["bam"])

        return "%s | %s | %s" % (aligner_cmd, sam_to_bam_cmd, bam_sort_cmd)

    def init_output_file_paths(self, **kwargs):
        split_id = kwargs.get("split_id", None)
        self.generate_output_file_path("bam", "sorted.bam", split_id=split_id)