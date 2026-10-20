ARG RAY_BASE_IMAGE=rayproject/ray:2.58.0-py311
FROM ${RAY_BASE_IMAGE}

USER root
WORKDIR /opt/dataflow
COPY pyproject.toml README.md ./
COPY src ./src
RUN python -m pip install --no-cache-dir '.[runtime]'

USER ray
WORKDIR /home/ray
