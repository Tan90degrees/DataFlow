FROM rayproject/ray:2.58.0-py311

ARG DATAFLOW_BUILD_COMMIT=""
ENV DATAFLOW_BUILD_COMMIT=$DATAFLOW_BUILD_COMMIT

USER root
WORKDIR /opt/dataflow

COPY pyproject.toml README.md ./
COPY src ./src

RUN python -m pip install --no-cache-dir .

USER ray
WORKDIR /home/ray
