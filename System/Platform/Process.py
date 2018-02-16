import subprocess as sp

class Process(sp.Popen):

    def __init__(self, args, **kwargs):
        self.command        = kwargs.pop("cmd",     True)
        self.num_retries    = kwargs.pop("num_retries", 0)
        super(Process, self).__init__(args,     **kwargs)
        self.complete = False

    def is_complete(self):
        return self.complete

    def set_complete(self):
        self.complete = True

    def has_failed(self):
        ret_code = self.poll()
        return ret_code is not None and ret_code != 0

    def get_command(self):
        return self.command

    def get_num_retries(self):
        return self.num_retries