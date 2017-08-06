import logging
import subprocess as sp
import json

class LogSink(object):
    # Google Cloud log sink object for directing log output to another Google Cloud component (i.e. PubSub topic)
    def __init__(self, name, dest, log_filter=None):

        self.name = name
        self.dest = dest

        # Filter to determine the types of messages that get processed by log sink
        self.filter = log_filter

        # Name of service account assocaited with log sink. Will be created by Google Cloud upon creation.
        self.serv_acct = None

        # Create the log sink on GooogleCloud
        self.create()

        # Determine name of log sink service account
        self.get_serv_acct()

    def _run_cmd(self, cmd, err_msg=None):
        # Wrapper around Popen to run command and wait for it to complete
        proc = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
        out, err = proc.communicate()

        # Check if any error has appeared
        if len(err) != 0 and "error" in err.lower():
            logging.error("Logging sink %s stopped working!" % self.name)
            if err_msg is not None:
                logging.error("%s. The following error appeared:\n    %s" % (err_msg, err))
                raise RuntimeError("LogSink failed!")

        return out

    def create(self):
        # Create a Google Cloud log sink using gcloud
        # Log sink will direct messages from cloud logging to the log destination (e.g. PubSub topic))
        opts = list()
        opts.append("--quiet")
        opts.append("--no-user-output-enabled")

        # Add filter string to capture only certain types of log entries
        opts.append("--log-filter=%s" % self.filter)

        cmd = "gcloud beta logging sinks create %s %s %s" % (self.name, self.dest, " ".join(opts))
        err_msg = "Could not create a logging sink on Google Stackdriver Logging"
        self._run_cmd(cmd, err_msg=err_msg)

    def destroy(self):
        # Remove log sink from Google Cloud
        cmd = "gcloud --quiet --no-user-output-enabled beta logging sinks delete %s" % self.name
        err_msg = "Could not destroy a logging sink on Google Stackdriver Logging"
        self._run_cmd(cmd, err_msg=err_msg)

    def get_serv_acct(self):
        # Get the name of the newly created service account associated with the log sink
        # Necesssary because log sinks automatically get their own service account
        if self.serv_acct is None:
            cmd = "gcloud beta logging sinks describe --format=json %s" % (self.name)
            err_msg = "Could not obtain information about a logging sink"

            out = self._run_cmd(cmd, err_msg=err_msg)

            self.serv_acct = json.loads(out)["writer_identity"].split(":")[1]
        else:
            # Return self.serv_acct if service account has already been determined
            return self.serv_acct