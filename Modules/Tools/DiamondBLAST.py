from Modules import Module

class DiamondBLAST(Module):
    def __init__(self, module_id):
        super(DiamondBLAST, self).__init__(module_id)

        self.input_keys = ["R1", "R2", "diamond", "diamond_db", "max_target_seqs", "nr_cpus", "mem"]

        self.output_keys = ["diamond_blast_output"]

    def define_input(self):
        self.add_argument("R1",                 is_required=True)
        self.add_argument("R2",                 is_required=False)
        self.add_argument("diamond",            is_required=True, is_resource=True)
        self.add_argument("diamond_db",         is_required=True, is_resource=True)
        self.add_argument("max_target_seqs",    is_required=True, default_value=1)
        self.add_argument("nr_cpus",            is_required=True, default_value="MAX")
        self.add_argument("mem",                is_required=True, default_value="nr_cpus * 4")

    def define_output(self, platform, split_name=None):

        # Generate an output file
        out_file = self.generate_unique_file_name(split_name=split_name, extension=".diamond.out")
        self.add_output(platform, "diamond_blast_output", out_file)

    def define_command(self, platform):

        # Get arguments to run Delly
        R1              = self.get_arguments("R1").get_value()
        R2              = self.get_arguments("R2").get_value()
        diamond         = self.get_arguments("diamond").get_value()
        diamond_db      = self.get_arguments("diamond_db").get_value()
        max_target_seqs = self.get_arguments("max_target_seqs").get_value()
        nr_cpus         = self.get_arguments("nr_cpus").get_value()
        mem             = self.get_arguments("mem").get_value()

        # Get output paths
        diamond_output  = self.get_output("diamond_blast_output")

        # Compute block size. One block size is roughly 6GB
        b = mem/6 - 10

        # Generate command for piping the sequencing reads
        if R1.endswith(".gz"):
            cmd1 = "zcat %s %s" % (R1, R2)
        else:
            cmd1 = "cat %s %s" % (R1, R2)

        # Generate diamond command
        cmd2 = "%s blastx --db %s --threads %s -b %s -o %s -f 6 --max-target-seqs %s !LOG3!" \
               % (diamond, diamond_db.split(".")[0], nr_cpus, b, diamond_output, max_target_seqs)

        return "%s | %s" % (cmd1, cmd2)
