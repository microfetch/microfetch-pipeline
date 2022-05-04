FROM python:3.10.4-slim@sha256:a2e8240faa44748fe18c5b37f83e14101a38dd3f4a1425d18e9e0e913f89b562

# Install Python requirements
WORKDIR /app
COPY requirements.txt /app/
RUN pip install -r requirements.txt

# Make user and copy files
RUN useradd --home-dir /app --user-group --no-create-home appuser
COPY --chown=appuser:appuser . .
USER appuser

# Set up the app environment
ENTRYPOINT python src/taxon_tracker.py -v --data-directory /app/data init && \
		python src/taxon_tracker.py -v --data-directory /app/data heartbeat
