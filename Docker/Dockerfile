# The first instruction is what image we want to base our container on
# We Use an official Python runtime as a parent image
FROM python:3.10-slim-bullseye

ENV HOME_DIR=/usr/src/backend

# The enviroment variable ensures that the python output is set straight
# to the terminal with out buffering it first
ENV PYTHONUNBUFFERED 1

# Install system dependencies. git is needed because there are some packages that are not available in PyPI
RUN apt-get update && apt-get -y install libpq-dev gcc jq wget curl git

# install uv
ADD https://astral.sh/uv/install.sh /install.sh
RUN chmod -R 755 /install.sh && /install.sh && rm /install.sh

RUN set -ex apt-get autoremove -y --purge wget && rm -rf /var/lib/apt/lists/*

RUN /root/.cargo/bin/uv pip install --system --upgrade pip

COPY requirements.txt /requirements.txt
RUN /root/.cargo/bin/uv pip install --system --no-cache -r /requirements.txt

RUN useradd -ms /bin/bash -d ${HOME_DIR} container_user
RUN chown -R container_user: ${HOME_DIR}

# Set the working directory to /app
WORKDIR ${HOME_DIR}

# Copy the current directory contents into the container at /app
COPY ddpui ${HOME_DIR}/app
COPY manage.py ${HOME_DIR}/
COPY .env ${HOME_DIR}/

VOLUME ${HOME_DIR}/app

# Make port 8000 available to the world outside this container
EXPOSE 8000