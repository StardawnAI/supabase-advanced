# Supabase Advanced — High Availability (HA)

Goal: run a self-hosted Supabase instance with one or more live standby
instances on other servers. Standbys replicate continuously (sub-second, not
nightly dumps) and take over automatically as the primary data source when the
current primary dies.

## Architecture

```
        clients + every Supabase service (auth, rest, realtime, storage, meta,
        functions, supavisor) — all of them connect via ${POSTGRES_HOST}
                                  |
                          [ pg-router : HAProxy ]
                                  |  health check: GET /primary on each agent
              +-------------------+--------------------+
              |                                        |
   server A                                 server B (different host)
   +---------------------+                  +---------------------+
   | db (postgres)       |  <== streaming   | db (postgres)       |
   | ha-agent :8008      |  replication ==> | ha-agent :8008      |
   +---------------------+   (slot, WAL)    +---------------------+
```

Key insight: the whole stack already reaches Postgres through the single
`POSTGRES_HOST` env var. Pointing it at `pg-router` makes failover transparent
to every service — nothing else needs to know a switch happened.

The router asks each node's agent `GET /primary`. Exactly one node answers 200
(the one where `pg_is_in_recovery()` is false), so traffic follows the primary
automatically. This is the Patroni/HAProxy pattern, without the Patroni+etcd
operational weight.

### Replication choice

Physical streaming replication with a replication slot — not logical
replication. Logical replication does not carry roles, DDL, extensions or the
`_supabase` internal database, so it cannot stand in for a whole Supabase
instance. Physical replication copies the entire cluster byte-for-byte.

### Split-brain

Two nodes cannot safely decide a failover alone: if only the *network between
them* breaks, the standby would promote itself while the primary is still
serving writes — two primaries, diverging data.

Mitigation implemented here:
- `HA_FAILOVER_MODE=manual` (default) — agent alarms, promotion is one click.
- `HA_FAILOVER_MODE=auto` — requires `HA_WITNESS_URLS`: the standby only
  promotes if an independent third party also cannot see the primary.
- Fencing: on promotion the agent drops the old primary out of the router
  before accepting writes.

## Checklist

- [x] 1. Sync fork to upstream → verify: `git rev-list --count upstream/master...HEAD` = 0
- [x] 2. `docs/PLAN.md` + `docs/STATUS.md`
- [ ] 3. `docker/ha/agent.py` — role/health/lag endpoints, promote, witness quorum
      → verify: unit-testable pure logic + live endpoint returns 200/503 correctly
- [ ] 4. `docker/ha/bootstrap-standby.sh` — pg_basebackup, slot, pgsodium key copy
      → verify: standby comes up in recovery and streams
- [ ] 5. `docker/ha/haproxy.cfg` — router with httpchk `/primary`
      → verify: `haproxy -c -f` passes; routes to primary only
- [ ] 6. `docker/docker-compose.ha.yml` (primary side) + `docker-compose.replica.yml` (standby side)
      → verify: `docker compose config` resolves
- [ ] 7. `docker/ha/setup-primary.sh` — replication role, pg_hba, slot
      → verify: `pg_stat_replication` shows the standby streaming
- [ ] 8. `run.sh ha …` commands (status, promote, replicas)
      → verify: `sh -n run.sh` + live run
- [ ] 9. Live two-node test: write on A → read on B; kill A → B promotes; router follows
      → verify: transcript of the run (this is the proof of done)
- [ ] 10. Studio UI: High Availability page (nodes, lag, promote button)
      → verify: typecheck + unit test
- [ ] 11. Docs: `docker/ha/README.md` + self-hosting guide page

## Non-goals (explicit)

- No automatic re-provisioning of a failed primary as a new standby beyond the
  documented `run.sh ha rejoin` path (it needs a fresh basebackup; that is a
  deliberate manual decision because it destroys the old node's data).
- No multi-master / write-anywhere. One primary at a time.
