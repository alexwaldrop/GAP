#!/usr/bin/env bash

# Waiting for all the locks to be released and installing the apt daemon
while [[ ! $(sudo apt-get install --yes aptdaemon) || ! $(aptdcon --version) ]]
do
    echo "Waiting for the apt-get locks to be released!"
    sleep 2
done

# Installing google-cloud-sdk in order to use the beta components
curl https://sdk.cloud.google.com | bash -s -- --disable-prompts --install-dir=/tmp/
gcloud_exec=/tmp/google-cloud-sdk/bin/gcloud
$gcloud_exec --quiet components install beta

# Send ready signal to the Pub/Sub
topic=`curl "http://metadata.google.internal/computeMetadata/v1/instance/attributes/ready-topic" -H "Metadata-Flavor: Google" 2>>/dev/null`
$gcloud_exec beta pubsub topics publish $topic "$(hostname)"