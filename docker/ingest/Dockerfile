# Ingest layer: the API that accepts documents and the worker that processes
# them. One image, two entry points, so both always run the same code.
#
# Dependency-free in the same sense as docker/ha: no pip install, no wheel
# tree to keep patched. The three things this needs beyond the standard
# library are command line tools with their own package maintenance —
# `psql` to reach Postgres and `pdftotext` to read PDFs.
FROM python:3.13-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        postgresql-client \
        poppler-utils \
        curl \
        procps \
    && rm -rf /var/lib/apt/lists/*

COPY db.py storage.py api.py worker.py admin.py fetcher.py /app/
COPY sql/ /app/sql/
# Shipped with the image so `run.sh ingest test` works on a machine that has
# docker but no local Python. Both suites, or the in-container run silently
# reports fewer tests than the repo has.
COPY test_ingest.py test_youtube.py /app/

WORKDIR /app

# Raw originals live here. Mounted as a volume by the compose overlay: losing
# them would mean re-fetching every document to improve a contract, which is
# the expensive half of the pipeline.
RUN mkdir -p /var/lib/ingest/raw && chown nobody /var/lib/ingest/raw
VOLUME ["/var/lib/ingest/raw"]

USER nobody
EXPOSE 8010

# Health is about the API answering. It reports a degraded status when the
# database is unreachable rather than failing, so a database outage shows up
# as a readable message instead of a container that keeps restarting.
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
    CMD ["python3", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8010/health', timeout=4)"]

# Unbuffered so job outcomes appear in `docker compose logs` immediately.
ENTRYPOINT ["python3", "-u"]
CMD ["/app/api.py"]
