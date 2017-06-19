import logging
import base64
import json
import threading
import time
import os
import subprocess as sp
from google.cloud import pubsub


from GAP_modules.Google import GoogleException
from GAP_modules.Google import Instance

class GooglePubSub(object):

    def __init__(self):

        self.topics = []
        self.subs = []

    @staticmethod
    def _run_cmd(cmd, err_msg=None):

        # Running and waiting for the command
        proc = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
        out, err = proc.communicate()

        # Check if any error has appeared
        if len(err) != 0 and "error" in err.lower():
            logging.error("Google Pub/Sub stopped working!")
            if err_msg is not None:
                logging.error("%s. The following error appeared:\n    %s" % (err_msg, err))
            raise GoogleException()

        return out

    def clean_up(self):

        while len(self.subs):
            self.delete_subscription(self.subs[0])

        while len(self.topics):
            self.delete_topic(self.topics[0])

    def create_subscription(self, subscription, topic):

        logging.debug("Creating subscription %s on Google Pub/Sub." % subscription)

        if topic not in self.topics:
            self.create_topic(topic)

        cmd = "gcloud --quiet --no-user-output-enabled beta pubsub subscriptions create %s --topic=%s" % (subscription, topic)
        err_msg = "Could not create a subscription"

        self._run_cmd(cmd, err_msg=err_msg)

        self.subs.append(subscription)

    def delete_subscription(self, subscription):

        logging.debug("Deleting subscription %s from Google Pub/Sub." % subscription)

        cmd = "gcloud --quiet --no-user-output-enabled beta pubsub subscriptions delete %s" % subscription
        err_msg = "Could not delete a subscription"

        self._run_cmd(cmd, err_msg=err_msg)

        self.subs.remove(subscription)

    def create_topic(self, topic):

        logging.debug("Creating topic %s on Google Pub/Sub." % topic)

        cmd = "gcloud --quiet --no-user-output-enabled beta pubsub topics create %s" % topic
        err_msg = "Could not create a topic on Google Pub/Sub"

        self._run_cmd(cmd, err_msg=err_msg)

        self.topics.append(topic)

    def delete_topic(self, topic):

        logging.debug("Deleting topic %s from Google Pub/Sub." % topic)

        cmd = "gcloud --quiet --no-user-output-enabled beta pubsub topics delete %s" % topic
        err_msg = "Could not delete a topic from Google Pub/Sub"

        self._run_cmd(cmd, err_msg=err_msg)

        self.topics.remove(topic)

    @staticmethod
    def grant_write_permission(topic, client_json_keyfile, serv_acct):
        #function used to grant write access to a service account (e.g. logging sink)
        try:
            #set google environmental variable
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = client_json_keyfile

            #get pubsub client with default service account
            client = pubsub.Client.from_service_account_json(client_json_keyfile)

            #get subscription
            topic_obj = client.topic(topic)

            #get IAM permissions for subscription
            policy = topic_obj.get_iam_policy()

            # Get the current list of editors
            topic_editors = policy.get("roles/editor", [])

            #add service account as an editor of the policy
            topic_editors.append(policy.service_account(serv_acct))
            policy["roles/editor"] = topic_editors

            #set policy for subscription
            topic_obj.set_iam_policy(policy)

        except BaseException as e:
            if e.message != "":
                logging.error("Could not set Pub/Sub permissions for topic %s. The following error appeared: %s." % (topic, e.message))
            raise

    @staticmethod
    def get_message(subscription):
        # Generating the command
        cmd = "gcloud beta pubsub subscriptions pull --auto-ack --max-messages=1 --format=json %s" % subscription
        err_msg = "Could not receive a message from Google Pub/Sub"

        out = GooglePubSub._run_cmd(cmd, err_msg=err_msg)

        # Parsing the output
        data = None
        attributes = None
        msg_json = json.loads(out)
        if len(msg_json) != 0:
            msg = msg_json[0]["message"]

            # Obtain the information
            data = msg.get("data", None)
            attributes = msg.get("attributes", None)

            # Decode the data
            if data is not None:
                data = base64.b64decode(data)

        return data, attributes

    @staticmethod
    def send_message(topic, message=None, attributes=None):

        # Checking the input
        if message is None and attributes is None:
            return

        # Parse the input
        message = "" if message is None else message
        attributes = {} if attributes is None else attributes

        # Parse the attributes
        attrs = ",".join([ "%s=%s" % (str(k), str(v)) for k,v in attributes.iteritems() ])

        # Generating the command
        cmd = "gcloud --quiet --no-user-output-enabled beta pubsub topics publish %s \"%s\" --attribute=%s" % (topic, message, attrs)
        err_msg = "Could not send a message to Google Pub/Sub"

        GooglePubSub._run_cmd(cmd, err_msg=err_msg)

class GoogleSubscriber(threading.Thread):

    def __init__(self, subscription, instances, status_to_send):
        super(GoogleSubscriber, self).__init__()

        self.instances = instances
        self.subscription = subscription
        self.status_to_send = status_to_send

        self.running = True

        self.daemon = True

    def run(self):

        while self.running:

            try:
                msg, _ = GooglePubSub.get_message(self.subscription)

                if msg is not None:
                    self.process_message(msg)
                else:
                    time.sleep(2)

            except BaseException, e:
                if self.running:
                    raise
                else:
                    if e.message != "":
                        logging.debug("Google Subscriber was forcefully stopped. The following exception message was received: %s." % e.message)
                    else:
                        logging.debug("Google Subscriber was forcefully stopped.")

    def stop(self):
        self.running = False

    def process_message(self, msg):
        raise NotImplementedError("Method \"process_message\" not implemented!")

class GoogleReadySubscriber(GoogleSubscriber):

    def __init__(self, subscription, instances):
        super(GoogleReadySubscriber, self).__init__(subscription, instances, Instance.AVAILABLE)

    def process_message(self, msg):

        # The message should be an instance
        try:
            if msg in self.instances:
                self.instances[msg].set_status(self.status_to_send)
                logging.debug("(%s) Instance ready!" % msg)
        except KeyError:
            logging.error("Ready message should be an instance! The following message was received instead: %s." % msg)
        except:
            raise

class GooglePreemptedSubscriber(GoogleSubscriber):

    def __init__(self, subscription, instances):
        super(GooglePreemptedSubscriber, self).__init__(subscription, instances, Instance.DEAD)

    def process_message(self, msg):

        # Message should be a Google log in JSON format
        try:
            log = json.loads(msg)
            inst_name = log["jsonPayload"]["resource"]["name"]
            if inst_name in self.instances:
                self.instances[inst_name].set_status(self.status_to_send)
                logging.debug("(%s) Instance preempted!" % inst_name)
        except ValueError:
            logging.error("Preempted message should be a Google log in JSON format. The following message was received instead: %s." % msg)
        except:
            raise