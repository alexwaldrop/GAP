from GAP_interfaces import Main
import time

class TaskManager(Main):
    
    def __init__(self, config, cluster):
        Main.__init__(self, config)

        self.config     = config
        self.cluster    = cluster 

        self.tasks      = {}

        self.queue      = []
        self.running    = []
        self.completed  = []

        self.max_nodes  = self.config.cluster.nodes
        self.max_cpus   = self.config.cluster.mincpus

        self.available_cpus     = [self.max_cpus] * self.max_nodes

    def addTask(self, new_task):
        self.tasks[new_task.name] = new_task

        # Identifying position for inserting tasks with the following order rules:
        #  1. increasing number of requirements
        #  2. decreasing resources requirements
        pos = 0
        for task_name in self.queue:
            current_task = self.tasks[task_name]
            if len(new_task.requires) > len(current_task.requires):
                pos += 1
            elif new_task.nodes <= current_task.nodes and \
                 new_task.mincpus <= current_task.mincpus:
                pos += 1
            else:
                break
            
        self.queue.insert(pos, new_task.name)

    def run(self):   
        while len(self.queue) + len(self.running):
            # Starting the first run
            if len(self.running) == 0:
                self.launchTask()

            # Checking the status of the running jobs
            for task_name in self.running:
                exit_status = self.cluster.checkStatus(self.tasks[task_name].job_id)

                # Checking if task is not running
                if exit_status != 2:
                    if exit_status == 0:
                        self.complete(task_name)
                        self.launchTask()
                        break
                    else:
                        self.error("A task was interrupted!")
            else:
                time.sleep(3)
            
    def launchTask(self): 
        # List of launched tasks
        launched = []

        # Launching tasks if possible
        for task_name in self.queue:
            task = self.tasks[task_name]

            for require in task.requires:
                if not self.tasks[require].is_done:
                    break
            else:
                # Searching available resources
                for i in range(self.max_nodes):
                    if task.mincpus <= self.available_cpus[i]:
                        # Ocupying resources
                        self.available_cpus[i] -= task.mincpus
                        task.which_node = i
 
                        # Marking as launched
                        launched.append(task_name)
                        self.running.append(task_name) 
                        
                        # Starting the process
                        task.start()

                        break

        for task_name in launched:
            self.queue.remove(task_name)

    def complete(self, task_name):
        # Moving the task to the completed list
        self.completed.append(task_name)
        self.running.remove(task_name)

        # Resetting resources
        which_node = self.tasks[task_name].which_node
        self.available_cpus[which_node] += self.tasks[task_name].mincpus
        
        # Marking as complete
        self.tasks[task_name].is_done = True

    def validate(self):
        pass

