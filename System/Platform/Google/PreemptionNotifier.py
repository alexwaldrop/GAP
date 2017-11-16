import logging
import json
import threading
import time

from LogSink import LogSink
from PubSub import PubSub
from GooglePreemptibleProcessor import GooglePreemptibleProcessor

class PreemptionNotifier(threading.Thread):

    def __init__(self, name, processors, key_file):
        super(PreemptionNotifier, self).__init__()

        self.name       = name

        # Dictionary of preemptible processors being watched by notifier
        self.processors = processors

        # Google cloud key file containing account metadata
        self.key_file   = key_file

        # Boolean flag indicating whether notifier is currently running
        self.running    = True

        # Boolean flag indicating that notifier is a daemon running in the background
        self.daemon     = True

        # Initialize PubSub topic/subscription for capturing/accesing logging info
        self.preempt_topic  = "preempted_topic_%s" % self.name
        self.preempt_sub    = "preempted_sub_%s" % self.name
        self.pub_sub        = None

        # Initialize log sink to funnel messages from Google Stackdriver logging to PubSub topic
        self.google_project     = self.__get_key_field("project_id")
        self.log_sink_name      = "preempted_sink_%s" % self.name
        self.log_sink_dest      = "pubsub.googleapis.com/projects/%s/topics/%s" % (self.google_project, self.preempt_topic)
        self.log_sink_filter    = "jsonPayload.event_subtype:compute.instances.preempted"
        self.log_sink           = None

    def __init_pubsub(self):
        # Create PubSub topics and subscriptions for storing/accessing information related to instance preemption
        logging.debug("Configuring Google Pub/Sub for preemption notifier...")
        pubsub = PubSub()

        # Create topic and subscription to that topic
        pubsub.create_topic(self.preempt_topic)
        pubsub.create_subscription(self.preempt_sub, self.preempt_topic)

        logging.info("Google Pub/Sub configured for preemption notifier.")
        return pubsub

    def __init_log_sink(self):
        # Create log sink to send preemption events to PubSub preempt topic
        logging.debug("Configuring Google Logging sink to send preemption events to Google Pub/Sub.")
        log_sink = LogSink(self.log_sink_name, self.log_sink_dest, log_filter=self.log_sink_filter)
        logging.info("Preemption logging sink '%s' successfully created!" % self.log_sink_name)

        # Grant write permission to allow the preempted log sink to write to the preempted PubSub topic
        self.pub_sub.grant_write_permission(topic=self.preempt_topic,
                                            client_json_keyfile=self.key_file,
                                            serv_acct=log_sink.get_serv_acct())
        return log_sink

    def run(self):
        # Periodically pull next message from PubSub subscription and check to see if instances have been preempted
        # If an instance has been preempted, set its instance status to DEAD

        # Create PubSub topics/subscription and log sinks
        self.pub_sub = self.__init_pubsub()
        self.log_sink = self.__init_log_sink()

        while self.running:
            try:
                # Periodically try to pull message from preempted subscription
                msg, _ = PubSub.get_message(self.preempt_sub)

                if msg is not None:
                    # Process the message if one was pulled from subscription
                    self.process_message(msg)
                else:
                    time.sleep(2)

            except BaseException, e:
                if self.running:
                    raise
                else:
                    if e.message != "":
                        logging.debug(
                            "Google Subscriber was forcefully stopped. The following exception message was received: %s."
                            % e.message)
                    else:
                        logging.debug("Google Subscriber was forcefully stopped.")

    def process_message(self, msg):
        # Process a message pulled from the Preempted PubSub Subscription
        # Message should be a Google log in JSON format
        try:
            log = json.loads(msg)
            inst_name = log["jsonPayload"]["resource"]["name"]
            if inst_name in self.processors:
                # Set instance status to DEAD if preempted instance is mangaged by the notifier
                self.processors[inst_name].set_status(GooglePreemptibleProcessor.DEAD)
                logging.warning("(%s) Instance preempted!" % inst_name)
        except ValueError:
            logging.error("Preempted message should be a Google log in JSON format. The following message was received instead: %s." % msg)
        except:
            raise

    def stop(self):
        self.running = False

    def clean_up(self):
        # Remove PubSub topics and remove log sink
        logging.info("Destroying preemption notifier...")
        self.stop()
        if self.log_sink is not None:
            # Try to delete log sink
            try:
                self.log_sink.destroy()
            except BaseException, e:
                logging.error("Unable to delete preemption log sink: %s" % self.log_sink_name)
                if e.message != "":
                    logging.error("The following error was received: %s" % e.message)
        if self.pub_sub is not None:
            # Try to delete PubSub subscription and topic
            try:
                self.pub_sub.clean_up()
            except BaseException, e:
                logging.error("Unable to completely delete Preemption Notifier Pub/Sub! Topic: %s. Subscription: %s." % (self.preempt_topic, self.preempt_sub))
                if e.message != "":
                    logging.error("The following error was received: %s" % e.message)

    def __get_key_field(self, field_name):
        # Parse JSON service account key file and return email address associated with account

        # Parse json into dictionary
        with open(self.key_file) as kf:
            key_data = json.load(kf)

        # Check to make sure correct key is present in dictionary
        if field_name not in key_data:
            logging.error("'%s' field missing from authentication key file: %s. Check to make sure key exists in file or that file is valid google key file!" %
                          (field_name, self.key_file))
            exit(1)
        return key_data[field_name]
