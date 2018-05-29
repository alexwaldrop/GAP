import logging

from Config import ConfigParser
from Task import Task

class Graph(object):

    def __init__(self, pipeline_config_file):

        # Parse and validate pipeline config
        pipeline_config_spec    = "System/Graph/Graph.validate"
        config_parser           = ConfigParser(pipeline_config_file, pipeline_config_spec)
        self.config             = config_parser.get_config()

        # Generate graph
        self.tasks, self.adj_list = self.__generate_graph()

        # Check validity of adjacency list
        self.__check_adjacency_list()

        # Check for cycles
        self.__check_cycles()


    def add_task(self, task):
        # Connect new node to existing graph
        if task.get_ID() in self.tasks:
            logging.error("Graph Error: Attempt to add duplicate task to graph: %s" % task.get_ID())
            raise RuntimeError("Cannot add task with duplicate ID to graph!")

        # Add new new to nodelist
        self.tasks[task.get_ID()] = task
        self.adj_list[task.get_ID()] = []

    def remove_task(self, task_id):
        # Remove node and all edges from Graph
        if task_id not in self.tasks:
            logging.error("Attempt to remove non-existant task from Graph: %s" % task_id)
            raise RuntimeError("Graph Error: Attempt to remove non-existant task from graph!")

        # Remove node from vertice list
        self.tasks.pop(task_id)
        self.adj_list.pop(task_id)

        # Remove all references to node in adjacency list
        for adj_list in self.adj_list.itervalues():
            if task_id in adj_list:
                adj_list.remove(task_id)

    def add_dependency(self, child_task_id, parent_task_id):
        # Adds dependency where dep_nod_id must wait until ind_node_id is finished
        if child_task_id not in self.tasks:
            logging.error("Unable to add dependency to graph! Unknown task: %s!" % child_task_id)
            raise RuntimeError("Attempt to add edge between non-existant tasks!")
        if parent_task_id not in self.tasks:
            logging.error("Unable to add dependency to graph! Unknown task: %s!" % parent_task_id)
            raise RuntimeError("Attempt to add edge between non-existant tasks!")

        # Add dependency
        self.adj_list[child_task_id].append(parent_task_id)

    def get_tasks(self, task_id=None):
        if task_id is None:
            return self.tasks
        return self.tasks[task_id]

    def get_unfinished_tasks(self):
        return [task for task in self.tasks if not task.is_complete()]

    def get_children(self, task_id):
        if task_id not in self.tasks:
            logging.error("Cannot list children for non-existant task: %s" % task_id)
            raise RuntimeError("Graph Error: Attempt to get children from nonexistant task!")
        dependents = []
        for task, edges in self.adj_list.iteritems():
            if task_id in edges:
                dependents.append(task)
        return dependents

    def get_parents(self, task_id):
        if task_id not in self.tasks:
            logging.error("Cannot list parent tasks for non-existant task: %s" % task_id)
            raise RuntimeError("Graph Error: Attempt to get parents from nonexistant task!")
        return self.adj_list[task_id]

    def is_complete(self):
        for task in self.tasks:
            if not task.is_complete():
                return False
        return True

    def parents_complete(self, task_id):
        # Determine if all task parents have completed
        parents = self.get_parents(task_id)
        return len(parents) == len([x for x in parents if self.get_tasks(x).is_complete()])

    def split_graph(self, splitter_task):
        # Recursively split tasks downstream of 'head_task' until a closing merge is reached
        child_tasks = self.get_children(splitter_task.get_ID())
        for split_id in splitter_task.get_output():
            # Create new graph partition for each new split
            split = splitter_task.get_output(split_id=split_id)
            # Get visible samples for new graph partition
            # If no visible samples declared, split nodes inherit visible samples from splitter task
            visible_samples = split["visible_samples"] if split["visible_samples"] is not None else splitter_task.get_visible_samples()
            for child_task in child_tasks:
                child_split = self.__split_subgraph(child_task, splitter_task, split_id, visible_samples)
                self.add_dependency(child_split.get_ID(), splitter_task.get_ID())

        # Remove deprecated tasks after all splits tasks have been created
        for task in self.__deprecated_tasks:
            self.remove_task(task)

    @property
    def __deprecated_tasks(self):
        return [task.get_ID() for task in self.tasks if task.is_deprecated()]

    def __generate_graph(self):

        tasks  = {}
        adj_list  = {}

        for task_id in self.config:

            task_data = self.config[task_id]

            adj_list[task_id] = task_data.pop("input_from")
            tasks[task_id] = Task(task_id, **task_data)

        return tasks, adj_list

    def __check_adjacency_list(self, runtime=False):
        errors = False
        for task, adj_tasks in self.adj_list.iteritems():
            for adj_task in adj_tasks:
                if adj_task not in self.tasks:
                    if not runtime:
                        logging.error("Incorrect pipeline graph! Node '%s' receives input from an undeclared task: '%s'. "
                                      "Please correct in the pipeline graph config!" % (task, adj_task))
                    else:
                        logging.error("Incorrect pipeline graph! Node '%s' receives input from an undeclared task: '%s'. "
                                      % (task, adj_task))
                    errors = True
        if errors:
            if not runtime:
                raise IOError("Incorrect pipeline graph defined in graph config!")
            else:
                raise RuntimeError("Runtime graph alteration resulted in invalid graph!")

    def __split_subgraph(self, task, splitter_task, split_id, visible_samples, level=1):
        # Recursively split subgraph that depends on 'task'
        if task.get_type() == "Merger":
            # Decrement current nesting scope after merge
            level -= 1

        if task.get_type() == "Splitter":
            # Increment current nesting scope after split
            level += 1

        if level == 0:
            # Current split has been merged (don't split downstream tasks)
            return task

        # Split current task into new task and add split to graph
        new_task_id = "%s.%s" % (task.get_ID(), split_id)
        split_task = task.split(new_task_id, splitter_task, split_id, visible_samples)
        self.add_task(split_task)

        # Create dependencies between current task and splits created for each child task
        child_tasks = self.get_children(task.get_ID())
        for child_task in child_tasks:
            # Split each child subgraph
            child_split = self.__split_subgraph(child_task, splitter_task, split_id, visible_samples, level)
            # Connect task to split child subgraph
            self.add_dependency(child_split.get_ID(), split_task.get_ID())

        if len(child_tasks) > 0:
            # Mark original task as deprecated so it can be discarded
            task.deprecate()

        # Return split task
        return split_task

    def __check_cycles(self, runtime=False):
        # Taken with modification from https://www.geeksforgeeks.org/detect-cycle-in-a-graph/
        cycle = False
        visited = []
        recStack = []
        for task_id in self.tasks.keys():
            if task_id not in visited:
                if self.__is_cycle(task_id, visited, recStack):
                    cycle = True
        if cycle:
            if not runtime:
                raise IOError("Incorrect pipeline graph: Cycle detected!")
            else:
                raise RuntimeError("Runtime graph alteration resulted in invalid graph: Cycle detected!")

    def __is_cycle(self, task_id, visited, recStack):

        # Mark current task as visited
        visited.append(task_id)
        # Add current task to current recursion stack
        recStack.append(task_id)

        # Check if any subgraph of current task contains a cycle
        neighbors = self.get_children(task_id)
        for neighbor_id in neighbors:
            if neighbor_id not in visited:
                if self.__is_cycle(neighbor_id, visited, recStack):
                    return True
            elif neighbor_id in recStack:
                logging.error("Incorrect pipeline graph: Cycle detected that includes task '%s'!" % task_id)
                return True

        # Pop current task from recursion stack
        recStack.remove(task_id)
        return False




