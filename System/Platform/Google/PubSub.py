import logging
import base64
import json
import os
import subprocess as sp
from google.cloud import pubsub

class PubSub(object):

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
            raise RuntimeError("Google Pub/Sub stopped working!")

        return out

    def clean_up(self):

        while len(self.subs):
            self.delete_subscription(self.subs[0])

        while len(self.topics):
            self.delete_topic(self.topics[0])

    def create_subscription(self, subscription, topic):
        # Function creates a Google Pub/Sub subscription to a Google cloud Pub/Sub topic
        logging.debug("Creating subscription %s on Google Pub/Sub." % subscription)

        # Creates Google Pub/Sub topic if it doesn't already exist
        if topic not in self.topics:
            self.create_topic(topic)

        # Create subscription to topic
        cmd = "gcloud --quiet --no-user-output-enabled beta pubsub subscriptions create %s --topic=%s" \
              % (subscription, topic)

        err_msg = "Could not create a subscription"
        self._run_cmd(cmd, err_msg=err_msg)

        # Add to list of subscriptions
        self.subs.append(subscription)

    def delete_subscription(self, subscription):
        # Delete a Google cloud Pub/Sub subscription
        logging.debug("Deleting subscription %s from Google Pub/Sub." % subscription)

        cmd = "gcloud --quiet --no-user-output-enabled beta pubsub subscriptions delete %s" % subscription
        err_msg = "Could not delete a subscription"
        self._run_cmd(cmd, err_msg=err_msg)

        # Remove from list of subscriptions
        self.subs.remove(subscription)

    def create_topic(self, topic):
        # Create Google cloud PubSub topic
        logging.debug("Creating topic %s on Google Pub/Sub." % topic)

        cmd = "gcloud --quiet --no-user-output-enabled beta pubsub topics create %s" % topic
        err_msg = "Could not create a topic on Google Pub/Sub"
        self._run_cmd(cmd, err_msg=err_msg)

        # Add to list of topics
        self.topics.append(topic)

    def delete_topic(self, topic):
        # Remove PubSub topic from Google cloud
        logging.debug("Deleting topic %s from Google Pub/Sub." % topic)

        cmd = "gcloud --quiet --no-user-output-enabled beta pubsub topics delete %s" % topic
        err_msg = "Could not delete a topic from Google Pub/Sub"
        self._run_cmd(cmd, err_msg=err_msg)

        # Remove from list of topics
        self.topics.remove(topic)

    @staticmethod
    def grant_write_permission(topic, client_json_keyfile, serv_acct):
        # Function used to grant write access to a service account (e.g. logging sink)
        try:
            # Set google environmental variable
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = client_json_keyfile

            # Get pubsub client with default service account
            client = pubsub.Client.from_service_account_json(client_json_keyfile)

            # Get subscription
            topic_obj = client.topic(topic)

            # Get IAM permissions for subscription
            policy = topic_obj.get_iam_policy()

            # Get the current list of editors
            topic_editors = policy.get("roles/editor", [])

            # Add service account as an editor of the policy
            topic_editors.append(policy.service_account(serv_acct))
            policy["roles/editor"] = topic_editors

            # Set policy for subscription
            topic_obj.set_iam_policy(policy)

        except BaseException as e:
            if e.message != "":
                logging.error("Could not set Pub/Sub permissions for topic %s. The following error appeared: %s."
                              % (topic, e.message))
            raise

    @staticmethod
    def get_message(subscription):
        # Function pops next message from a PubSub subscription
        # Decodes message and returns message contents
        cmd = "gcloud beta pubsub subscriptions pull --auto-ack --max-messages=1 --format=json %s" % subscription
        err_msg = "Could not receive a message from Google Pub/Sub"
        out = PubSub._run_cmd(cmd, err_msg=err_msg)

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
        # Send a message to an existing Google cloud Pub/Sub topic

        # Return if message and attributes are both empty
        if message is None and attributes is None:
            return

        # Parse the input message and attributes
        message = "" if message is None else message
        attributes = {} if attributes is None else attributes

        # Parse the attributes and pack into a single data structure message
        attrs = ",".join(["%s=%s" % (str(k), str(v)) for k,v in attributes.iteritems()])

        # Run command to publish message to the topic
        cmd = "gcloud --quiet --no-user-output-enabled beta pubsub topics publish %s \"%s\" --attribute=%s" \
              % (topic, message, attrs)

        err_msg = "Could not send a message to Google Pub/Sub"
        PubSub._run_cmd(cmd, err_msg=err_msg)
        