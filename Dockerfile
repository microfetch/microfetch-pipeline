FROM python:3.10.4-slim@sha256:a2e8240faa44748fe18c5b37f83e14101a38dd3f4a1425d18e9e0e913f89b562

# Install bash and wget and Java runtime
RUN apt-get update && \
    apt-get install -y \
    wget \
    openjdk-11-jre-headless && \
    apt-get purge && \
    apt-get clean && \
		apt-get autoclean && \
		apt-get autoremove

# Install nextflow
RUN cd /bin && \
    wget -qO- https://get.nextflow.io | bash && \
    chmod +x nextflow

# Install Python requirements
COPY ./requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Make user and copy files
RUN useradd --home-dir /app --user-group --no-create-home appuser
WORKDIR /app
COPY --chown=appuser:appuser ./app .
USER appuser

# Set up the app environment
ENTRYPOINT python src/taxon_tracker.py -v --data-directory /app/data init && \
		(python src/taxon_tracker.py -v --data-directory /app/data serve &) && \
		python src/taxon_tracker.py -v --data-directory /app/data heartbeat
