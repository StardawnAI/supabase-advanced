# HA agent: health probes, replication status and failover for one Postgres node.
#
# Deliberately dependency-free — the agent uses only the Python standard
# library plus the psql/pg_isready clients, so there is no wheel tree to keep
# patched on a machine whose job is to stay up.
FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY agent.py /app/agent.py

USER nobody
EXPOSE 8008

# Unbuffered so failover decisions show up in `docker compose logs` at once.
ENTRYPOINT ["python3", "-u", "/app/agent.py"]
