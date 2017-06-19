import logging
import json
import subprocess as sp

from GAP_modules.Google import GoogleException

class LogSink(object):

    def __init__(self, sink_name, dest, log_filter=""):

        self.name = sink_name
        self.dest = dest

        self.filter = log_filter

        self.serv_acct = None

        self.create()

        self.get_serv_acct()

    def _run_cmd(self, cmd, err_msg=None):

        # Running and waiting for the command
        proc = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
        out, err = proc.communicate()

        # Check if any error has appeared
        if len(err) != 0 and "error" in err.lower():
            logging.error("Logging sink %s stopped working!" % self.name)
            if err_msg is not None:
                logging.error("%s. The following error appeared:\n    %s" % (err_msg, err))
            raise GoogleException()

        return out

    def create(self):

        opts = list()
        opts.append("--quiet")
        opts.append("--no-user-output-enabled")
        opts.append("--log-filter=%s" % self.filter)

        cmd = "gcloud beta logging sinks create %s %s %s" % (self.name, self.dest, " ".join(opts))
        err_msg = "Could not create a logging sink on Google Stackdriver Logging"

        self._run_cmd(cmd, err_msg=err_msg)

    def destroy(self):

        cmd = "gcloud --quiet --no-user-output-enabled beta logging sinks delete %s" % self.name
        err_msg = "Could not destroy a logging sink on Google Stackdriver Logging"

        self._run_cmd(cmd, err_msg=err_msg)

    def get_serv_acct(self):

        cmd = "gcloud beta logging sinks describe --format=json %s" % (self.name)
        err_msg = "Could not obtain information about a logging sink"

        out = self._run_cmd(cmd, err_msg=err_msg)

        self.serv_acct = json.loads(out)["writer_identity"].split(":")[1]


class GoogleLogging(object):

    def __init__(self):

        self.sinks = dict()

    def create_sink(self, sink_name, dest, log_filter=""):
        self.sinks[sink_name] = LogSink(sink_name, dest, log_filter=log_filter)

    def get_serv_acct(self, sink_name):
        if sink_name not in self.sinks:
            return None
        return self.sinks[sink_name].serv_acct

    def clean_up(self):

        # Destroying all the Logging sinks
        for sink in self.sinks:
            self.sinks[sink].destroy()
