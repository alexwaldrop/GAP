# CloudConductor ![alt text](http://via.placeholder.com/50x50 "Logo Title Text 1")

## What is CloudConductor?

**CloudConductor** is a cloud-based workflow engine for defining and executing bioinformatics pipelines in a cloud environment. 
Currently, the framework has been tested extensively on the [Google Cloud Platform](https://cloud.google.com/), but will eventually support other platforms including AWS, Azure, etc.

## Feature Highlights

  * **User-friendly**
    * Define complex workflows by linking together user-defined modules that can be re-used across pipelines
    * [Config_obj](http://configobj.readthedocs.io/en/latest/configobj.html) for clean, readable workflows (see below example)
    * +50 pre-installed modules for existing bioinformatics tools
  * **Portable**
    * Docker integration ensures reproducible runtime environment for modules    
    * Platform independent (currently supports GCS; AWS, Azure to come)
  * **Modular/Extensible**
    * User-defined Plug-N-Play modules
      * Re-used across pipelines, re-combined in any combination
      * Modules easily added, customized as new tools needed, old tools changed
      * Eliminates copy/paste re-use of code across workflows 
  * **Pre-Launch Type-Checking**
    * Strongly-typed module declarations allow catching pipeline errors before they occur
    * Pre-launch checks make sure all external files exist before runtime
  * **Scalable**
    * Removes resource limitations imposed by cluster-based HPCCs
  * **Elastic**
    * VM usage automatically scales to match input file sizes, computational needs
  * **Scatter-Gather Parallelism**
    * In-built logic for dividing large tasks into small chunks and re-combining
  * **Economical**
    * Preemptible/Spot instances drastically cut workflow costs

## Setting up your system
  
CloudConductor is currently designed only for *Linux* systems. 
You will need to install and configure the following tools to run your pipelines on Google Cloud:  

1. [Python](https://www.python.org/) v2.7.*

    You can check your Python version by running the following command in your terminal:

    ```sh
    $ python -V
    Python 2.7.10
    ```

    To install the correct version of Python, visit the official Python [website](https://www.python.org/downloads/).

2. Python packages: *configobj*, *jsonschema*, *requests*

    You will need [pip](https://packaging.python.org/guides/installing-using-linux-tools/) to install the above packages.
    After installing *pip*, run the following commands in your terminal: 

    ``` sh
    # Upgrade pip
    sudo pip install -U pip
    
    # Install Python modules
    sudo pip install -U configobj jsonschema requests
    ```

3. [Google Cloud Platform](https://cloud.google.com/) SDK

    Follow the [instructions](https://cloud.google.com/sdk/docs/downloads-interactive) on the official Google Cloud website.

## Usage

  For more information about CloudConductor and how to use it check the [documentation](https://google.com).

        usage: CloudConductor [-h] --input SAMPLE_SET_CONFIG --name PIPELINE_NAME
                              --pipeline_config GRAPH_CONFIG --res_kit_config
                              RES_KIT_CONFIG --plat_config PLATFORM_CONFIG --plat_name
                              PLATFORM_MODULE [-v] -o FINAL_OUTPUT_DIR
        
        optional arguments:
          -h, --help            show this help message and exit
          --input SAMPLE_SET_CONFIG
                                Path to config file containing input files and information for one or more samples.
          --name PIPELINE_NAME  Descriptive pipeline name. Will be appended to final output dir. Should be unique across runs.
          --pipeline_config GRAPH_CONFIG
                                Path to config file defining pipeline graph and tool-specific input.
          --res_kit_config RES_KIT_CONFIG
                                Path to config file defining the resources used in the pipeline.
          --plat_config PLATFORM_CONFIG
                                Path to config file defining platform where pipeline will execute.
          --plat_name PLATFORM_MODULE
                                Platform to be used. Possible values are:
                                   Google (as module 'GooglePlatform')
          -v                    Increase verbosity of the program.Multiple -v's increase the verbosity level:
                                   0 = Errors
                                   1 = Errors + Warnings
                                   2 = Errors + Warnings + Info
                                   3 = Errors + Warnings + Info + Debug
          -o FINAL_OUTPUT_DIR, --output_dir FINAL_OUTPUT_DIR
                                Absolute path to the final output directory.
                                
## A simple pipeline example
Below, we use CloudConductor's in-built scatter-gather logic to align a set of reads to a reference genome. 
```ini

# Trim input FASTQ reads with Trimmomatic
[trim_reads]
	module      = Trimmomatic

# Scatter trimmed FASTQ into smaller chunks for fast alignment
[split_fastq]
	module      = FastqSplitter
	input_from  = trim_reads

# Align chunks in parallel to reference genome using BWA
[align_reads]
	module      = BWA
	input_from  = split_fastq, get_read_group

# Index BAM files output by BWA
[index_bam]
	module      = Samtools
	submodule   = Index
	input_from  = align_reads

#  Merge split BAM files into single bam
[merge_bams]
	module      = MergeBams
	input_from  = align_reads, index_bam

```

## What Next?

Get started with our full [documentation](https://google.com) to explore the ways CloudConductor can streamline the development and execution of complex, multi-sample workflows typical in bioinformatics.

## Project Status

CloudConductor is actively under development. To get involved or request features, please any of the authors listed below.

## Authors

* [Alex Waldrop](https://github.com/alexwaldrop)
* [Razvan Panea](https://github.com/ripanea)
* [Tushar Dave](https://github.com/tushardave26)
