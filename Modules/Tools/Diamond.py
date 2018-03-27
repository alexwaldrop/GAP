from Modules import Module

class Diamond(Module):
    def __init__(self, module_id):
        super(Diamond, self).__init__(module_id)

        self.input_keys = ["R1", "R2", "diamond", "taxonmap", "taxonnodes", "diamond_db", "nr_cpus", "mem"]

        self.output_keys = ["diamond_output"]

    def define_input(self):
        self.add_argument("R1",             is_required=True)
        self.add_argument("R2",             is_required=False)
        self.add_argument("diamond",        is_required=True, is_resource=True)
        self.add_argument("taxonmap",       is_required=True, is_resource=True)
        self.add_argument("taxonnodes",     is_required=True, is_resource=True)
        self.add_argument("diamond_db",     is_required=True, is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value="MAX")
        self.add_argument("mem",            is_required=True, default_value="nr_cpus * 2")

    def define_output(self, platform, split_name=None):

        # Generate an output file
        out_file = self.generate_unique_file_name(split_name=split_name, extension=".diamond.out")
        self.add_output(platform, "diamond_output", out_file)

    def define_command(self, platform):

        # Get arguments to run Delly
        R1              = self.get_arguments("R1").get_value()
        R2              = self.get_arguments("R2").get_value()
        diamond         = self.get_arguments("diamond").get_value()
        taxonmap        = self.get_arguments("taxonmap").get_value()
        taxonnodes      = self.get_arguments("taxonnodes").get_value()
        diamond_db      = self.get_arguments("diamond_db").get_value()
        nr_cpus         = self.get_arguments("nr_cpus").get_value()
        mem             = self.get_arguments("mem").get_value()

        # Get output paths
        diamond_output  = self.get_output("diamond_output")

        # Compute block size. One block size is roughly 6GB
        b = mem/6 - 2

        # Generate command for piping the sequencing reads
        if R1.endswith(".gz"):
            cmd1 = "zcat %s %s" % (R1, R2)
        else:
            cmd1 = "cat %s %s" % (R1, R2)

        # Generate diamond command
        cmd2 = "%s blastx --taxonmap %s --taxonnodes %s --db %s --threads %s -b %s -o %s -f 102 !LOG3!" \
               % (diamond, taxonmap, taxonnodes, diamond_db.split(".")[0], nr_cpus, b, diamond_output)

        return "%s | %s" % (cmd1, cmd2)
