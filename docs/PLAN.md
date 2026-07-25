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
  promotes if an independent third party also cannot see the primary. Without
  witnesses configured it refuses to promote at all.
- The router treats the first node in `HA_NODES` as preferred and the rest as
  backups, so if two nodes ever claim to be primary, traffic stays with the
  original one instead of flapping.
- The overview page reports a split brain when more than one node claims to be
  primary. It cannot be fenced automatically across a partition — that is the
  nature of the problem — so it is made visible instead.

## Checklist

- [x] 1. Sync fork to upstream → verify: `git rev-list --count upstream/master...HEAD` = 0
- [x] 2. `docs/PLAN.md` + `docs/STATUS.md`
- [x] 3. `docker/ha/agent.py` — role/health/lag endpoints, promote, witness quorum
      → 37 unit tests pass; live endpoints return 200/503 per role
- [x] 4. `docker/ha/bootstrap-standby.sh` — pg_basebackup, slot, pgsodium key warning
      → standby comes up in recovery and streams (lag 0.27s)
- [x] 5. `docker/ha/render-haproxy.sh` — router with httpchk `/primary`
      → `haproxy -c -f` passes; only the primary receives writes
- [x] 6. `docker/docker-compose.ha.yml` (primary) + `docker/standby.compose.yml` (standby)
      → `docker compose config` resolves; both run in the live test
- [x] 7. `docker/ha/setup-primary.sh` — replication role, pg_hba, verification
      → `pg_stat_replication` shows the standby streaming
- [x] 8. `run.sh ha …` commands (init, status, promote, nodes, keys)
      → `sh -n` clean; promote exercised end to end in the manual-mode run
- [x] 9. Live two-node test: write on A → read on B; kill A → B promotes; router follows
      → **auto 26/26, manual 27/27.** Promotion 6s after the kill, router 0s,
        no committed row lost. Split-brain detection confirmed by restarting
        the old primary.
- [x] 10. Overview page (nodes, lag, promote button) — served by the agent
      rather than built into Studio, see Deviations below
- [x] 11. Docs: `docker/ha/README.md` + `.env.example` section

## Deviations from the original plan

**The overview page is served by the agent, not built into Studio.** Three
reasons: it keeps working when the database it reports on is down; it is
reachable on every node, including a standby whose primary has vanished; and it
keeps this feature out of a Studio codebase that this fork pulls thousands of
upstream commits into at a time. A page inside Studio is still possible on top
of this — the agents expose plain JSON — but it would mean carrying Studio
patches across every sync.

## Non-goals (explicit)

- No automatic re-provisioning of a failed primary as a new standby beyond the
  documented `run.sh ha rejoin` path (it needs a fresh basebackup; that is a
  deliberate manual decision because it destroys the old node's data).
- No multi-master / write-anywhere. One primary at a time.
