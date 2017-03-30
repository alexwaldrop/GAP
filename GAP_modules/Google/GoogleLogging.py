import logging
import json
import subprocess as sp

from GAP_modules.Google import GoogleException

class LogSink(object):

    def __init__(self, sink_name, dest, log_filter="", unique_id=False):

        self.name = sink_name
        self.dest = dest

        self.filter = log_filter

        self.unique_id = unique_id
        self.serv_acct = None

        self.create()

        if self.unique_id:
            self.get_serv_acct()
            self.grant_permission()

    @staticmethod
    def _run_cmd(cmd, err_msg=None):

        # Running and waiting for the command
        proc = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
        out, err = proc.communicate()

        # Check if any error has appeared
        if len(err) != 0 and "error" in err.lower():
            logging.error("Logging sink %s stopped working!")
            if err_msg is not None:
                logging.error("%s. The following error appeared:\n    %s" % (err_msg, err))
            raise GoogleException()

        return out

    def create(self):

        opts = list()
        opts.append("--quiet")
        opts.append("--no-user-output-enabled")
        opts.append("--output-version-format=V2")
        opts.append("--log-filter=%s" % self.filter)
        if not self.unique_id:
            opts.append("--no-unique-writer-identity")

        cmd = "gcloud beta logging sinks create %s %s %s" % (self.name, self.dest, " ".join(opts))
        err_msg = "Could not create a logging sink on Google Stackdriver Logging"

        self._run_cmd(cmd, err_msg=err_msg)

    def destroy(self):

        if self.unique_id:
            self.revoke_permission()

        cmd = "gcloud --quiet --no-user-output-enabled beta logging sinks delete %s" % self.name
        err_msg = "Could not destroy a logging sink on Google Stackdriver Logging"

        self._run_cmd(cmd, err_msg=err_msg)

    def get_serv_acct(self):

        cmd = "gcloud beta logging sinks describe --format=json %s"
        err_msg = "Could not obtain information about a logging sink"

        out = self._run_cmd(cmd, err_msg=err_msg)

        self.serv_acct = json.loads(out)["writer_identity"]

    def grant_permission(self):

        cmd = "gcloud projects add-iam-policy-binding davelab-gcloud --member %s --role roles/pubsub.editor" % self.serv_acct
        err_msg = "Could not grant permissions for the logging sink"

        self._run_cmd(cmd, err_msg=err_msg)

    def revoke_permission(self):

        cmd = "gcloud project remove-iam-policy-binding davelab-gcloud --member %s --role roles/pubsub.editor" % self.serv_acct
        err_msg = "Could not revoke permissions for the logging sink"

        self._run_cmd(cmd, err_msg=err_msg)


class GoogleLogging(object):

    def __init__(self):

        self.sinks = []

    def create_sink(self, sink_name, dest, log_filter=""):
        self.sinks.append(LogSink(sink_name, dest, log_filter=log_filter))

    def clean_up(self):

        # Destroying all the Logging sinks
        for sink in self.sinks:
            sink.destroy()