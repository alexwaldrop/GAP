# Write a tool module for GAP framework

This guide will help new developer to build and submit a new tool module to GAP framework.

The framewrok uses [Git] as version control system. The entire codebase is hosted on Duke's [GitLab] website. If you are not familiar with Git, please refer to this [Git Guide] to get basic familiarity with it.

## Workflow diagram of tool development process
![workflow diagram][diagram]

## Requirements
  1. Git
  2. Most up to date developer branch of the framework

## Steps to follow
  1. Create an issue on GitLab (e.g. Implement Annovar Module)
  2. If you are developing your first tool module, you might not have to clone the framework repository. In that case, please clone the repository by executing the following command line under your favourite directory.
  ```sh
  $ git clone git@gitlab.oit.duke.edu:davelab/GAP.git
  ```
  **_NOTE_**: You are required to set up the SSH KeyGen before the execution of above command line. Please refer to this [guide][SSH KeyGen Guide] to set up the SSH KeyGen.
  3. Make sure that you are currently on the most up to date **developer** branch. To check that execute following command in your `Terminal`
  ```sh
  $ git branch
  
  --OUTPUT--
  On branch develop
  Your branch is up-to-date with 'origin/develop'.
  ```
  4. Make a feature specific branch
  ```sh
  $ git branch <feature_branch>
  ```
  5. Change brach to feature branch
  ```sh
  $ git checkout <feature_branch>
  ```
  6. Make sure the change of branch
  ```sh
  $ git branch
  
  --OUTPUT--
  develop
* <feature_branch>
  ```
  7. Create a tool module specific `Python` file under `Modules/Tools/`

    Every tool module class constructor method contains following three attributes:
    |       Attribute            |       Type          |       Description     |
    |:--------------------------:|:-------------------:|-----------------------|
    |`input_keys`                |          `List`     |**Ask Alex/Razvan to explain this**|
    |`output_keys`               |          `List`     |Ouput file type (e.g. VCF)|
    |`quick_command`             |          `Bool`     |Indicated to framework to run the tool on new instance or on the parent instance|

    Every tool module class contains three methods:
    |       Method            |                        Description                         |
    |:------------------------:|------------------------------------------------------------|
    |`define_input`            | define all the input arguments to the tool and the platform|
    |`define_output`           | generate the uniq tool specific output file name |
    |`define_command`          | get all the tool specific arguments and build the command line for the tool|

  8. Add tool specific resources in `ResourceKit.config` file
  9. Make tool specific sampleset file in `json`
  10. Make tool specific `Graph.config` file
  11. Test the tool module by executing the following command line. Please make sure you are executing the command line from the framework root directory.
  ```sh
  $ python ./Main.py
  ```

# To Do
  1. Add workflow diagram

[//]: # (These are reference links used in the body of this note and get stripped out when the markdown processor does its job. There is no need to format nicely because it shouldn't be seen.)


   [Git]: <https://github.com/joemccann/dillinger>
   [diagram]: <http://via.placeholder.com/350x150>
   [GitLab]: <https://gitlab.oit.duke.edu/davelab/GAP>
   [Git Guide]: <http://rogerdudler.github.io/git-guide/>
   [SSH KeyGen Guide]: <https://www.cyberciti.biz/faq/how-to-set-up-ssh-keys-on-linux-unix/>
   [Ace Editor]: <http://ace.ajax.org>
   [node.js]: <http://nodejs.org>
   [Twitter Bootstrap]: <http://twitter.github.com/bootstrap/>
   [jQuery]: <http://jquery.com>
   [@tjholowaychuk]: <http://twitter.com/tjholowaychuk>
   [express]: <http://expressjs.com>
   [AngularJS]: <http://angularjs.org>
   [Gulp]: <http://gulpjs.com>

   [PlDb]: <https://github.com/joemccann/dillinger/tree/master/plugins/dropbox/README.md>
   [PlGh]: <https://github.com/joemccann/dillinger/tree/master/plugins/github/README.md>
   [PlGd]: <https://github.com/joemccann/dillinger/tree/master/plugins/googledrive/README.md>
   [PlOd]: <https://github.com/joemccann/dillinger/tree/master/plugins/onedrive/README.md>
   [PlMe]: <https://github.com/joemccann/dillinger/tree/master/plugins/medium/README.md>
   [PlGa]: <https://github.com/RahulHP/dillinger/blob/master/plugins/googleanalytics/README.md>
