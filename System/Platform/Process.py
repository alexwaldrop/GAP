import subprocess as sp

class Process(sp.Popen):

    def __init__(self, args, **kwargs):
        self.command        = kwargs.pop("cmd",     True)
        self.num_retries    = kwargs.pop("num_retries", 0)
        self.docker_image   = kwargs.pop("docker_image", None)
        # Quiet failure means logger will not register command failure as error
        self.quiet          = kwargs.pop("quiet_failure", False)
        self.log_success    = kwargs.pop("log_success", True)
        super(Process, self).__init__(args,     **kwargs)
        self.complete       = False
        self.stopped        = False
        self.out            = ""
        self.err            = ""

    def is_complete(self):
        return self.complete

    def set_complete(self):
        self.complete = True

    def set_output(self, out, err):
        self.out = out
        self.err = err

    def has_failed(self):
        ret_code = self.poll()
        return ret_code is not None and ret_code != 0

    def get_command(self):
        return self.command

    def get_num_retries(self):
        return self.num_retries

    def get_docker_image(self):
        return self.docker_image

    def get_output(self):
        return self.out, self.err

    def is_quiet(self):
        return self.quiet

    def stop(self):
        self.stopped = True
        self.terminate()

    def is_stopped(self):
        return self.stopped

    def do_log_success(self):
        return self.log_success