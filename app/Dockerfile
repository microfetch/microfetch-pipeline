FROM python:3.10.4-slim@sha256:a2e8240faa44748fe18c5b37f83e14101a38dd3f4a1425d18e9e0e913f89b562

# Install wget for healthchecking
RUN apt-get update && \
    apt-get install -y wget && \
    apt-get autoremove && \
    apt-get autoclean

# Install Python requirements
WORKDIR /app
COPY requirements.txt /app/
RUN pip install -r requirements.txt

# Make user and copy files
RUN useradd --home-dir /app --user-group --no-create-home appuser
COPY --chown=appuser:appuser . .
USER appuser
