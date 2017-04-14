import logging

from GAP_interfaces import Tool

__main_class__= "BwaAligner"

class BwaAligner(Tool):
    
    def __init__(self, config, sample_data):
        super(BwaAligner, self).__init__()

        self.config = config
        self.sample_data = sample_data

        self.bwa            = self.config["paths"]["bwa"]
        self.samtools       = self.config["paths"]["samtools"]
        self.ref            = self.config["paths"]["ref"]

        self.temp_dir       = self.config["general"]["temp_dir"]

        self.sample_name    = self.sample_data["sample_name"]

        self.can_split      = True
        self.splitter       = "FASTQSplitter"
        self.merger         = "SamtoolsBAMMerge"

        self.nr_cpus        = self.config["platform"]["max_nr_cpus"]    # BWA MEM can use as many CPUs as possible
        self.mem            = max(10, self.nr_cpus)    # BWA MEM should not need more than 1 GB/CPU

        self.R1             = None
        self.R2             = None
        self.split_id       = None

    def get_rg_header(self):

        if "read_group_tag" in self.sample_data:
            return self.sample_data["read_group_tag"]

        # Obtain the read header
        cmd = "head -n 1 %s" % self.R1
        out, err = self.sample_data["main-server"].run_command("fastq_header", cmd, log=False, get_output=True)
        if err != "":
            err_msg = "Could not obtain information for BwaAligner. "
            err_msg += "\nThe following command was run: \n  %s" % cmd
            err_msg += "\nThe following error appeared: \n  %s" % err
            logging.error(err_msg)
            exit(1)

        # Generating the read group information
        fastq_header_data = out.lstrip("@").strip("\n").split(":")
        id = ":".join(fastq_header_data[0:4])
        pu = fastq_header_data[-1]
        sm = self.sample_data["sample_name"]
        lb = self.sample_data["lib_name"]
        pl = self.sample_data["seq_platform"]

        self.sample_data["read_group_tag"] = "\\t".join( ["@RG", "ID:%s" % id, "PU:%s" % pu, "SM:%s" % sm, "LB:%s" % lb, "PL:%s" % pl] )

        # Generating the read group header
        return self.sample_data["read_group_tag"]

    def get_command(self, **kwargs):

        # Obtaining the arguments
        self.R1                 = kwargs.get("R1",              self.sample_data["R1"])
        self.R2                 = kwargs.get("R2",              self.sample_data["R2"])
        self.nr_cpus            = kwargs.get("nr_cpus",         self.nr_cpus)
        self.mem                = kwargs.get("mem",             self.mem)
        self.split_id           = kwargs.get("split_id",        None)

        # Generating command for alignment
        aligner_cmd = "%s mem -M -R \"%s\" -t %d %s %s %s !LOG2!" % (self.bwa, self.get_rg_header(), self.nr_cpus, self.ref, self.R1, self.R2)

        # Generating command for converting SAM to BAM
        sam_to_bam_cmd  = "%s view -uS -@ %d - !LOG2!" % (self.samtools, self.nr_cpus)

        # Generating the output
        self.output = "%s/%s" % (self.temp_dir, self.sample_name)
        if self.split_id is not None:
            self.output += "_%d.bam" % self.split_id
        else:
            self.output += ".bam"
            self.sample_data["bam"] = self.output

        # Generating command for sorting BAM
        bam_sort_cmd = "%s sort -@ %d - -o %s !LOG3!" % (self.samtools, self.nr_cpus, self.output)

        return "%s | %s | %s" % (aligner_cmd, sam_to_bam_cmd, bam_sort_cmd)
