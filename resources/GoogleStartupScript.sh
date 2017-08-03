#!/usr/bin/env bash

# Waiting for all the locks to be released and installing the apt daemon
while [[ ! $(sudo apt-get install --yes aptdaemon) || ! $(aptdcon --version) ]]
do
    echo "Waiting for the apt-get locks to be released!"
    sleep 2
done

############################################################################################*
###################### DO NOT EDIT COMMANDS ABOVE THIS SECTION #############################*
############### ADDITIONAL COMMANDS TO BE EXECUTED CAN BE PLACED BELOW #####################*
############################################################################################*



