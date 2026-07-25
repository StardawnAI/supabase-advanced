# HA agent: health probes, replication status and failover for one Postgres node.
#
# Deliberately dependency-free — the agent uses only the Python standard
# library plus the psql/pg_isready clients, so there is no wheel tree to keep
# patched on a machine whose job is to stay up.
FROM python:3.13-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY agent.py /app/agent.py

USER nobody
EXPOSE 8008

# Health is about the agent answering, not about the database being up: a node
# whose Postgres has died is exactly when this agent matters most, and killing
# it then would take away the only thing that can report the outage.
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
    CMD ["python3", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8008/health', timeout=4)"]

# Unbuffered so failover decisions show up in `docker compose logs` at once.
ENTRYPOINT ["python3", "-u", "/app/agent.py"]
