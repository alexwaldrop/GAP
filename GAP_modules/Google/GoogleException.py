import logging

class GoogleException(Exception):

    def __init__(self, instance_name=None):

        if instance_name is not None:
            logging.error("(%s) Instance has failed!" % instance_name)
            exit(1)

