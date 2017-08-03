import os
import logging
import abc
from collections import OrderedDict
import subprocess as sp

from Process import Process

class Processor(object):
    __metaclass__ = abc.ABCMeta
    def __init__(self, name, nr_cpus, mem, **kwargs):
        self.name       = name
        self.nr_cpus    = nr_cpus
        self.mem        = mem

        # Get name of directory where logs will be written
        self.log_dir    = kwargs.pop("log_dir", None)

        # Ordered dictionary of processing being run by processor
        self.processes  = OrderedDict()

    def create(self):
        pass

    def destroy(self):
        pass

    def run(self, job_name, cmd):

        # Checking if logging is required
        if "!LOG" in cmd:

            # Generate name of log file
            log_file = "%s.log" % job_name
            if self.log_dir is not None:
                log_file = os.path.join(self.log_dir, log_file)

            # Generating all the logging pipes
            log_cmd_null    = " >>/dev/null 2>&1 "
            log_cmd_stdout  = " >>%s " % (log_file)
            log_cmd_stderr  = " 2>>%s " % (log_file)
            log_cmd_all     = " >>%s 2>&1 " % (log_file)

            # Replacing the placeholders with the logging pipes
            cmd = cmd.replace("!LOG0!", log_cmd_null)
            cmd = cmd.replace("!LOG1!", log_cmd_stdout)
            cmd = cmd.replace("!LOG2!", log_cmd_stderr)
            cmd = cmd.replace("!LOG3!", log_cmd_all)

        # Make any modifications to the command to allow it to be run on a specific platform
        cmd = self.__adapt_cmd(cmd)

        # Run command using subprocess popen and add Popen object to self.processes
        logging.info("(%s) Process '%s' started!" % (self.name, job_name))
        logging.debug("(%s) Process '%s' has the following command:\n    %s" % (self.name, job_name, cmd))

        # Generating process arguments
        kwargs = dict()

        # Process specific arguments
        kwargs["cmd"] = cmd

        # Popen specific arguments
        kwargs["shell"] = True
        kwargs["stdout"] = sp.PIPE
        kwargs["stderr"] = sp.PIPE

        self.processes[job_name] = Process(cmd, **kwargs)

    def wait(self):
        # Returns when all currently running processes have completed
        for proc_name, proc_obj in self.processes.iteritems():
            self.wait_process(proc_name)

    @abc.abstractmethod
    def wait_process(self, proc_name):
        pass

    @abc.abstractmethod
    def set_env_variable(self, env_variable, path):
        pass

    @abc.abstractmethod
    def __adapt_cmd(self, cmd):
        pass


