
class Argument(object):

    def __init__(self, name, is_required=False, is_resource=False, default_value=None):

        self.__name = name

        self.__is_required = is_required
        self.__is_resource = is_resource

        self.__default_value = default_value

        self.__value = None

    def set(self, value):
        self.__value = value

    def get_name(self):
        return self.__name

    def get_default_value(self):
        return self.__default_value

    def get_value(self):
        return self.__value

    def is_set(self):
        return self.__value is not None

    def is_mandatory(self):
        return self.__is_required

    def is_resource(self):
        return self.__is_resource
