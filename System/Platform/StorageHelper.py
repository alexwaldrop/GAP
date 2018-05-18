import logging

class InvalidStorageTypeError(Exception):
    pass

class StorageHelper(object):
    # Class designed to facilitate remote file manipulations for a processor

    @staticmethod
    def mv(src_path, dest_dir):
        # Transfer a remote file from src_path to a local directory dest_dir
        # Log the transfer unless otherwise specified
        storage_handler = StorageHelper.get_storage_handler_obj(src_path, dest_dir)
        return storage_handler.mv(src_path, dest_dir)

    @staticmethod
    def mkdir(dir_path):
        # Makes a directory if it doesn't already exists
        storage_handler = StorageHelper.get_storage_handler_obj(dir_path)
        return storage_handler.mkdir(dir_path)

    @staticmethod
    def get_file_size(path):
        # Determine file size
        storage_handler = StorageHelper.get_storage_handler_obj(path)
        return storage_handler.get_file_size(path)

    @staticmethod
    def get_storage_handler_obj(src_path, dest_path=None):
        # Determine the class of file handler to use base on input file protocol types

        # Get file storage protocol for src, dest files
        src_protocol = StorageHelper.get_file_protocol(src_path)
        dest_protocol = None if dest_path is None else StorageHelper.get_file_protocol(dest_path)
        protocols = [src_protocol, dest_protocol]

        # Remove 'NoneType' protocol
        protocols.remove(None)

        # Remove 'Local' protocol
        protocols.remove("Local")

        # If no other protocols remain then use local storage handler
        if len(protocols) == 0:
            return LocalStorageHandler

        # Cycle through file handlers to see which ones satisfy file protocol type required

        # Get available storage handlers
        storage_handlers = StorageHandler.__subclasses__()
        for storage_handler in storage_handlers:
            if storage_handler.PROTOCOL.lower() in protocols:
                return storage_handler

        # Raise error because we can't handle the type of file currently
        logging.error("StorageHelper cannot handle one or more input file storage types!")
        logging.error("Path: %s" % src_path)
        if dest_path is not None:
            logging.error("Dest_path: %s" % dest_path)
        raise InvalidStorageTypeError("Cannot handle input file storage type!")

    @staticmethod
    def get_file_protocol(path):
        if ":" not in path:
            return "Local"
        return path.split(":")[0]


class StorageHandler(object):
    PROTOCOL = None


class LocalStorageHandler(StorageHandler):

    PROTOCOL = "Local"

    @staticmethod
    def mv(src_path, dest_dir):
        # Move a file from one directory to another
        return "sudo mv %s %s" % (src_path, dest_dir)

    @staticmethod
    def mkdir(dir_path):
        # Makes a directory if it doesn't already exists
        return "sudo mkdir -p %s" % dir_path

    @staticmethod
    def get_file_size(path):
        # Return cmd for getting file size in bytes
        return "sudo du -sh --apparent-size --bytes %s" % path


class GoogleStorageHandler(StorageHandler):

    PROTOCOL = "gs"

    @staticmethod
    def mv(src_path, dest_dir):
        # Move a file from one directory to another
        options_fast = '-m -o "GSUtil:sliced_object_download_max_components=200"'
        return "gsutil %s cp -r %s %s" % (options_fast, src_path, dest_dir)

    @staticmethod
    def mkdir(dir_path):
        # Makes a directory if it doesn't already exists
        return "touch dummy.txt ; gsutil cp dummy.txt %s" % dir_path

    @staticmethod
    def get_file_size(path):
        # Return cmd for getting file size in bytes
        return "gsutil du -s %s" % path
