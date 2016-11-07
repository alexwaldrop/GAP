'''
Created on Oct 20, 2016

@author: alex
'''

import os.path
import subprocess as sp
from Interfaces import Main
from  configobj import *
from validate import Validator
from Main.GlobalVariables import DEFAULT_CONFIG_SPEC_FILE, REF_GENOME_IMAGE_BUCKET, GOOGLE_JSON_KEY


class Config(Main):


    def __init__(self, config_file, config_spec_file, silent = False):

        Main.__init__(self, self)
        
        if silent:
            self.error  = self.warning

        #set config file
        self.config_file = config_file
        
        #set config spec file from global variable list (Main.GlobalVariables.py)
        if (config_spec_file != None):
            self.config_spec_file = config_spec_file
        else:
            self.config_spec_file = DEFAULT_CONFIG_SPEC_FILE
        
        #read config file 
        self.config = self.read_config_file(self.config_file, self.config_spec_file)
        
        #validate runtime configuration
        self.valid = self.validate(self.config)
        
    
    
    def read_config_file(self, config_file, config_spec_file):

        # Checking if the config file exists
        if not os.path.isfile(self.config_file):
            self.error("Config file not found!")
        
        if not os.path.isfile(self.config_spec_file):
            self.error("Config specification file not found!") 
        
        #attempt to parse config file
        try:
            #self.config.readfp(open(self.config_file))
            self.config=ConfigObj(self.config_file, configspec=self.config_spec_file)
        except:
            self.error("Config parsing error! Invalid config file format.")
        return(self.config)
    
    
    
    def validate_config_schema(self, config_parser):
        
        #validate schema
        validator = Validator()
        results = config_parser.validate(validator)
        
        #report errors with file and throw error
        if results != True:
            error_string = "Invalid config error!\n"
            for (section_list, key, _) in flatten_errors(config_parser, results):
                if key is not None:
                    error_string += 'The "%s" key in the section "%s" failed validation\n' % (key, ', '.join(section_list))
                else:
                    print 'The following section was missing:%s \n' % ', '.join(section_list)
            self.error(error_string)
            return(False)
        #return true otherwise
        else:
            return(True)
    
      
        
    def authenticate(self):
        
        #check if key file exists
        self.message("Authenticating to the Google Cloud.")
        if not os.path.exists(GOOGLE_JSON_KEY):
            self.error("Authentication key was not found!")

        #run command to get google authorization using JSON-formatted Google service account key
        cmd = "gcloud auth activate-service-account --key-file %s" % GOOGLE_JSON_KEY
        proc = sp.Popen(cmd, shell = True)

        #return wait status
        if proc.wait() != 0:
            self.error("Authentication to Google Cloud failed!")
        else:
            self.message("Authentication to Google Cloud was successful.")
    
    
    
    def validate(self, config_parser):
        
        valid = True
        
        #validate config file structure
        try:
            valid = self.validate_config_schema(config_parser)
        except:
            return(False)
        
        return(valid)

        
        
    
            
            
    
    
    

