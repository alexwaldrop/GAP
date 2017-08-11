import abc

class ABCMetaEnhanced(abc.ABCMeta):

    def __call__(cls, *args, **kwargs):
        # noinspection PyArgumentList
        obj = abc.ABCMeta.__call__(cls, *args, **kwargs)
        obj.check_init()

        return obj