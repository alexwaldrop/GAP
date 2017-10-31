# Getting Starterd

This guide will help you with the followings:
  1. Setting up your system for [GAP] framework
  2. Design and implement your first pipeline
  3. Execute your first pipeline

### Setting up your system

The GAP framework is designed and implemented using following technologies:
  1. [Python] v2.7>
  2. [Google Cloud Platform] ([Beta] Version)

#### For UNIX/Linux or MAC OS X Users

Almost, every UNIX/Linux distribution comes with pre-installed Python. To check that, please open `Terminal` and execute below command in your shell.

```sh
$ python -V

--OUTPUT--
Python 2.7.10
```

If you see a similar output as above then the Python is already installed. Otherwise, please download and install the Python 2.7 or greater from [here][Python]

The framework haeavily utilize standard and custom modules. Please install the following Python modules using `pip` - a Python module manager as shown below.

``` sh
# upgrade pip, setuptools, and wheel Python modules
$ sudo pip install -U pip setuptools wheel

# install configobj Python module
$ sudo pip install configobj

# install jsonschema Python module
$ sudo pip install jsonschema

# install requests Python module
$ sudo pip install requests

# install Google Cloud SDK
$ sudo pip install google-cloud
```

#### CAUTION
When you are using system Python 2.7 on MAC OS X El Capitan and Sierra, you might experience errors with `google-cloud` Python module. Both the OS contains very strong security restrictions. If you experience errors regarding `six-1.4.1-py2.7.egg-info`, please execute below command in your `Terminal`. For more information about the issue, please read this GitHub issue [thread][python-six-issue]. It is always recommended to use custom Python distribution such as [Anaconda] because messing up with system Python can cause big troubles. 
```sh
$ sudo pip install google-cloud --ignore-installed six
```
Download and install Google Cloud SDK. Please follow the prompt after executing the first command line.
```sh
# install gcloud
$ sudo curl https://sdk.cloud.google.com | bash

# restart your shell
$ exec -l $SHELL

# confirm the installation
$ gcloud version

--OUTPUT--
Google Cloud SDK 168.0.0
bq 2.0.25
core 2017.08.21
gcloud 
gsutil 4.27
```

The fraework is devloped using `gcloud beta` so please install the `gcloud beta` using following command line in `Terminal`.
```sh
$ gcloud components install beta
```

The framework is codebase maintained using [Git] - a version control system and hosted on Duke's [GitLab]. Please make sure that your system have Git installed. To do so, please execute followin command in yout `Termial`.

```sh
$ git --version

--OUTPUT--
git version 2.9.2
```

If you see similar output in your `Terminal` then the Git is available at your disposable. Otherwise, please download and install it from [here][Git].

Please clone the framework codebase to your local system using following series of commands.

```sh
# make a new directory
$ mkdir GAP

# change the directory
$ cd GAP

# clone the framework codebase
# clone the repo via HTTPS
$ git clone https://gitlab.oit.duke.edu/davelab/GAP.git

#clone the repo via SSH
$ git clone git@gitlab.oit.duke.edu:davelab/GAP.git
```

#### For Windows Users
Unfortuantely, the GAP framework is not compatible with Windows native platorms. However, you can use the framework with the Linux virtual machine (VM). To install and configure Linux virtual machine on Windows, please refer to this [guide]. After successful installation of VM, please start the VM and follow the steps explained [here](#For-UNIX/Linux-or-MAC-OS-X-Users).

### Write your first pipeline
  * Write about making a new Pipeline graph config
  * Write about making a new sample JSON file

We are going to use the resources available to us. If you wish to create a new resource, please edit `Resource_kit.config` that comes with the framework codebase.  

# TO DOS
  1. Finish the [Write your first pipeline](#Write your first pipeline) section

[//]: # (These are reference links used in the body of this note and get stripped out when the markdown processor does its job. There is no need to format nicely because it shouldn't be seen.)

   [GAP]: <https://davelab.org/gap>
   [Python]: <https://www.python.org/>
   [Google Cloud Platform]: <https://cloud.google.com/>
   [Beta]: <https://cloud.google.com/python/>
   [guide]: <https://www.lifewire.com/install-ubuntu-linux-windows-10-steps-2202108>
   [Git]: <https://git-scm.com/>
   [python-six-issue]: <https://github.com/pypa/pip/issues/3165>
   [Anaconda]: <https://www.anaconda.com/download/>