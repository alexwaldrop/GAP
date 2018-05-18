import logging

from System.Platform import StorageHelper

class ModuleExecutor(object):

    def __init__(self, task_id, processor, workspace):
        self.task_id        = task_id
        self.processor      = processor
        self.workspace      = workspace
        self.storage_helper = StorageHelper
        self.docker_image   = None

    def load_input(self, inputs):

        # Create workspace directory structure
        self.__create_workspace()

        # Load input files
        # Inputs: list containing remote files, local files, and docker images
        seen = []
        for task_input in inputs:

            # Case: Pull docker image if it hasn't already been pulled
            if task_input.has_metadata("docker_image"):
                docker_image = task_input.get_metadata("docker_image")
                if docker_image not in seen:
                    self.storage_helper.pull_docker_image(docker_image)
                    seen.append(docker_image)

            # Case: Transfer file into wrk directory if its not already there
            elif task_input.get_transferrable_path() not in seen:

                # Transfer file to workspace directory
                src_path = task_input.get_transferrable_path()
                self.storage_helper.mv(src_path, dest_dir=self.workspace.get_wrk_dir())

                # Add transfer path to list of remote paths that have been transferred to local workspace
                seen.append(src_path)

                # Update path after transferring to wrk directory
                task_input.update_path(new_dir=self.workspace.get_wrk_dir())

            elif task_input.get_transferrable_path() in seen:
                # Update path after transferring to wrk directory
                task_input.update_path(new_dir=self.workspace.get_wrk_dir())

        # Recursively give every permission to all files we just added
        logging.info("(%s) Final workspace perm. update for task '%s'..." % self.processor.name, self.task_id)
        self.__grant_workspace_perms(job_name="grant_final_wrkspace_perms")

        # Wait for all processes to finish
        self.processor.wait()

    def run(self, cmd):
        job_name = self.task_id
        self.processor.run(job_name, cmd, docker_image=self.docker_image)
        return self.processor.wait_process(job_name)

    def save_output(self, outputs, final_output_types):
        # Return output files to workspace output dir

        # Get workspace places for output files
        final_output_dir = self.workspace.get_final_output_dir()
        tmp_output_dir = self.workspace.get_tmp_output_dir()

        for output_file in outputs:
            if output_file.get_type() in final_output_types:
                dest_dir = final_output_dir
            else:
                dest_dir = tmp_output_dir

            # Calculate output file size
            file_size = self.storage_helper.get_file_size(output_file)
            output_file.set_size(file_size)

            # Transfer to correct output directory
            self.storage_helper.mv(output_file, dest_dir)

            # Update path of output file to reflect new location
            output_file.update_path(new_dir=dest_dir)

    def save_logs(self):
        # Move log directory to final output log directory
        tmp_log_dir = self.workspace.get_wrk_log_dir()
        final_log_dir = self.workspace.get_final_log_dir()
        self.storage_helper.mv(tmp_log_dir, final_log_dir, log_transfer=False)

    def __create_workspace(self):
        # Create all directories specified in task workspace

        logging.info("(%s) Creating workspace for task '%s'..." % (self.processor.name, self.task_id))
        self.storage_helper.mkdir(self.workspace.get_wrk_dir())
        self.storage_helper.mkdir(self.workspace.get_log_dir())
        self.storage_helper.mkdir(self.workspace.get_tmp_output_dir())
        self.storage_helper.mkdir(self.workspace.get_final_output_dir())
        self.storage_helper.mkdir(self.workspace.get_final_log_dir())

        # Set processor wrk, log directories
        self.processor.set_wrk_dir(self.workspace.get_wrk_dir())
        self.processor.set_log_dir(self.workspace.get_log_dir())

        # Give everyone all the permissions on working directory
        logging.info("(%s) Updating workspace permissions..." % self.processor.name)
        self.__grant_workspace_perms(job_name="grant_initial_wrkspace_perms")

        # Wait for all the above commands to complete
        self.processor.wait()
        logging.info("(%s) Successfully created workspace for task '%s'!" % self.processor.name, self.task_id)

    def __grant_workspace_perms(self, job_name):
        cmd = "sudo chmod -R 777 %s" % self.workspace.get_wrk_dir()
        self.processor.run(job_name=job_name, cmd=cmd)



