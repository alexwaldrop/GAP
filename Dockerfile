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
RUN apt-get update

# upgrade pip, setuptools, and wheel Python modules
RUN pip install -U pip setuptools wheel configobj jsonschema requests

# Install gcloud
RUN curl https://sdk.cloud.google.com | bash &\
    exec -l $SHELL &\
    gcloud components install beta

# Install gcloud
RUN mkdir GAP &\
    cd GAP &\
    git clone &\
    git clone https://github.com/alexwaldrop/GAP.git

ENV PATH /GAP:$PATH

CMD ["GAP.py"]