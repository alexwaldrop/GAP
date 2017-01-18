import subprocess as sp

class GoogleProcess(sp.Popen):

    def __init__(self, args, **kwargs):

        self.instance_id = kwargs.pop("instance_id", None)

        super(GoogleProcess, self).__init__(args, **kwargs)

        self.complete = False
        if isinstance(args, list):
            self.command = " ".join(args)

        else:
            self.command = args

    def is_done(self):

        return self.poll() is not None

    def has_failed(self):

        ret_code = self.poll()
        return ret_code is not None and ret_code != 0

    def get_command(self):
        return self.command

    def get_instance_id(self):
        return self.instance_id