# Status

Running log for the high-availability work. Newest first.

## 2026-07-25 — HA feature complete on `feat/ha-replication`

### Fork brought up to date

The fork had **no commits of its own** and was 5039 behind upstream, last synced
2025-09-13. Fast-forwarded to upstream `2b27ed0ab1` (2026-07-24) and pushed.
Nothing could be lost — there was nothing local to lose. No fork-sync GitHub
Action exists in this repo; the sync was done from the console.

### What was built

Streaming replication with automatic failover for the self-hosted stack.

| Piece | File |
| --- | --- |
| Agent: roles, lag, promotion, witness quorum, overview page | `docker/ha/agent.py` |
| Standby clone | `docker/ha/bootstrap-standby.sh` |
| Primary preparation | `docker/ha/setup-primary.sh` |
| Router config generator | `docker/ha/render-haproxy.sh` |
| CLI behind `run.sh ha` | `docker/ha/ha-cli.sh` |
| Primary overlay | `docker/docker-compose.ha.yml` |
| Standby stack (second server) | `docker/standby.compose.yml` |
| Unit tests (37) | `docker/ha/test_agent.py` |
| End-to-end test | `docker/tests/test-ha-failover.sh` |
| Documentation | `docker/ha/README.md` |

### Key decisions

**Route through `POSTGRES_HOST`.** Every Supabase service already reaches
Postgres through that one variable. Pointing it at a router makes failover
invisible to all of them, with no per-service change.

**Physical, not logical replication.** A standby has to carry roles, DDL,
extensions and the `_supabase` database. Logical replication reproduces none of
those.

**Auto-failover requires witnesses, and refuses without them.** Two nodes cannot
distinguish a dead primary from a broken link between them. Promoting during a
split leaves two primaries whose data cannot be merged. Default is manual.

**The overview page lives in the agent, not in Studio.** It stays up when the
database it reports on is down, works on every node, and keeps the feature out
of a Studio codebase this fork re-syncs from upstream in bulk. This is the one
place the delivered shape differs from "a page inside Studio" — see Open below.

### Bugs found by testing, and fixed

Each of these would have failed on a first real deployment:

1. **`pg_reload_conf()` is silently refused.** In Supabase `postgres` is not a
   superuser (only `supabase_admin` is), so the freshly added replication rule
   was never loaded. Now the postmaster is signalled directly, and a failure is
   fatal rather than ignored.
2. **`pg_promote()` is refused for the same reason.** The standby detected the
   outage, got witness agreement, decided to promote — and was rejected every
   two seconds. Agents now connect as `supabase_admin`.
3. **`hot_standby=off` in the Supabase image.** A node in recovery then refuses
   every connection, making the standby unusable: no reads, no health check, no
   role query. Standbys now start with `hot_standby=on`.
4. **The readiness check tested the wrong connection type.** Postgres matches
   logical replication connections against ordinary `host all all` rules, so the
   check passed while the clone still failed. It now opens the physical
   connection `pg_basebackup` uses.
5. **HAProxy permanently disabled unresolvable nodes.** A standby added later
   would never be routed to. Fixed with a runtime resolver.
6. **Missing healthchecks broke `run.sh start`**, which uses `up --wait`.
7. **The router needed a `start_period`** — it resolves every node twice at
   startup, so binding can take ~20s.

### Proof

- Unit tests: **37 passed** (`python3 -m unittest discover -s docker/ha`).
- Automatic failover, two nodes plus witness and router on stardawneg64:
  **24/24 checks passed.** Replication lag 0.27s, standby promoted itself
  **6s** after the primary was killed, router followed in 0s, no committed row
  lost.
- Manual mode: **27/27 checks passed.** The standby did *not* promote itself, a
  promote without a valid token was refused with 401, and the operator's
  promotion through the agent worked.

Tested against `supabase/postgres:15.8.1.048` because that image was already on
the host. The stack default is 17.6.1.136; the mechanism is version-independent,
but the pair must match.

### Open

- **Studio integration.** Delivered as a standalone page served by the agent
  (reasoning above). A page inside Studio remains possible if wanted; it would
  mean carrying Studio patches across upstream syncs.
- **Failover covers the database only.** If a whole server disappears, something
  still has to point clients at the other server's Kong — a DNS or
  load-balancer job outside this stack.
- **Replication is asynchronous by default**, so a failover can lose the last
  transactions that had not reached the standby. Setting
  `synchronous_standby_names` trades write latency for not losing them.
