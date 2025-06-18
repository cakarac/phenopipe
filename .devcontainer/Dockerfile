FROM ubuntu:22.04

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive && apt-get -y install pip git curl zip

RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="${PATH}:/root/.local/bin"

WORKDIR /workspaces/pipe
