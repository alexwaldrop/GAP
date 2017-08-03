import subprocess as sp

class Process(sp.Popen):

    def __init__(self, args, **kwargs):
        self.command    = kwargs.pop("cmd",     True)
        super(Process, self).__init__(args,     **kwargs)

    def is_complete(self):
        return self.complete

    def set_complete(self):
        self.complete = True

    def has_failed(self):
        ret_code = self.poll()
        return ret_code is not None and ret_code != 0

    def get_command(self):
        return self.command