# Base Image
FROM ubuntu:16.04

# Metadata
LABEL base.image="gap:latest"
LABEL version="1"
LABEL software="GAP"
LABEL software.version="latest"
LABEL description="Bioinformatics cloud workflow management system."
LABEL tags="NGS Cloud GAP GoogleCloud AWS Bioinformatics Workflow Pipeline"

# Maintainer
MAINTAINER Alex Waldrop <alex.waldrop@duke.edu>

# update the OS related packages
RUN apt-get update -y &&\
    apt-get install -y python-pip curl git

# upgrade pip, setuptools, and wheel Python modules
RUN pip install -U pip setuptools wheel configobj jsonschema requests

# Install gcloud
RUN curl -sSL https://sdk.cloud.google.com > /tmp/gcl &&\
    bash /tmp/gcl --disable-prompts &&\
    echo "if [ -f '/root/google-cloud-sdk/path.bash.inc' ]; then source '/root/google-cloud-sdk/path.bash.inc'; fi" >> /root/.bashrc &&\
    echo "if [ -f '/root/google-cloud-sdk/completion.bash.inc' ]; then source '/root/google-cloud-sdk/completion.bash.inc'; fi" >> /root/.bashrc
ENV PATH /root/google-cloud-sdk/bin:$PATH

# Install gcloud beta components for pubsub
RUN /bin/bash -c "gcloud components install beta --quiet"

# Install gcloud
RUN git clone https://github.com/alexwaldrop/GAP.git

ENV PATH /GAP:$PATH

CMD ["GAP.py"]