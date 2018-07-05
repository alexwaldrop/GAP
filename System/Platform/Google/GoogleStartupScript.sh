#!/usr/bin/env bash

# Waiting for all the locks to be released and installing the apt daemon
while [[ ! $(sudo apt-get update ; sudo apt-get install --yes aptdaemon) || ! $(aptdcon --version) ]]
do
    echo "Waiting for the apt-get locks to be released!"
    sleep 2
done

# Updating the repositories
sudo aptdcon --hide-terminal -c

############################################################################################*
###################### DO NOT EDIT COMMANDS ABOVE THIS SECTION #############################*
############### ADDITIONAL COMMANDS TO BE EXECUTED CAN BE PLACED BELOW #####################*
############################################################################################*





############################################################################################*
###################### DO NOT EDIT COMMANDS BELOW THIS SECTION #############################*
############### ADDITIONAL COMMANDS TO BE EXECUTED CAN BE PLACED ABOVE #####################*
############################################################################################*

# Signal that instance is fully initialized
ZONE=$(curl "http://metadata.google.internal/computeMetadata/v1/instance/zone" -H "Metadata-Flavor: Google" | cut -d "/" -f 4)
gcloud --quiet compute instances add-metadata $(hostname) --metadata READY=TRUE --zone ${ZONE}