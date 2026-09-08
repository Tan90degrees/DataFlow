FROM rayproject/ray:2.58.0-py311

USER root
WORKDIR /opt/dataflow

COPY pyproject.toml README.md ./
COPY src ./src

RUN python -m pip install --no-cache-dir .

USER ray
WORKDIR /home/ray
