# High availability for self-hosted Supabase

Run your database on more than one server. A standby on a second machine
replicates the primary continuously — seconds behind, not a nightly dump — and
takes over as the write target when the primary dies.

```
   clients + every Supabase service (auth, rest, realtime, storage,
   meta, functions, pooler) — all of them via ${POSTGRES_HOST}
                            |
                    [ pg-router ]
                            |  health check: GET /primary
            +---------------+----------------+
            |                                |
      server A                          server B
  +-------------------+            +-------------------+
  | db (primary)      | ==WAL==>   | db (standby)      |
  | ha-agent :8008    |            | ha-agent :8008     |
  +-------------------+            +-------------------+
```

The whole stack already reaches Postgres through one setting, `POSTGRES_HOST`.
Point that at the router and a failover becomes invisible to every service —
none of them needs to know which machine is currently primary.

The router asks each node's agent `GET /primary`. Exactly one answers 200: the
node where `pg_is_in_recovery()` is false. Traffic follows that answer, so a
failover needs no config change and no restart.

---

## Before you start

- **The Postgres image must be identical on both servers.** A standby is a
  byte-level copy of the primary and will not start against a different build.
- **The replication port must be reachable from the standby**, and should not
  be reachable from anywhere else. It is a direct line into Postgres. Put it on
  a private network, a VPN, or a firewall rule that only allows the other
  server.
- **Automatic failover needs a witness.** See [Split-brain](#split-brain).

---

## Setting up the primary

From `docker/` on the first server:

```sh
sh run.sh config add ha    # adds the router and the agent to the stack
sh run.sh ha init          # generates the API token, points POSTGRES_HOST at the router
sh run.sh start
sh run.sh ha setup-primary # creates the replication role, opens pg_hba, verifies it
```

`ha setup-primary` prints the values the standby needs, including the generated
`HA_REPLICATION_PASSWORD`.

### If the primary already holds data

Nothing above touches your data — the router and agent are added alongside the
running database. `POSTGRES_HOST` changing to `pg-router` does mean the services
reconnect, so expect a short blip when you restart the stack.

---

## Setting up a standby

On the second server, clone this repository, then in `docker/`:

```sh
cp .env.example .env
```

Set these in the standby's `.env`:

| Variable | Meaning |
| --- | --- |
| `HA_NODE_NAME` | A name for this node, e.g. `node-b` |
| `HA_PRIMARY_HOST` | Address of the primary, reachable from here |
| `HA_PRIMARY_PORT` | The primary's published replication port (`HA_DB_REPLICATION_PORT`, default `5434`) |
| `HA_REPLICATION_PASSWORD` | From `ha setup-primary` on the primary |
| `HA_API_TOKEN` | The same token as the primary |
| `POSTGRES_PASSWORD` | The same password as the primary |
| `JWT_SECRET` | The same secret as the primary |
| `HA_FAILOVER_MODE` | `manual` (default) or `auto` |

Then:

```sh
docker compose -f standby.compose.yml up -d
```

The first start clones the primary with `pg_basebackup`, which takes as long as
your database is large. Afterwards the node streams continuously. Restarting it
resumes replication; it does not clone again.

### Copy the pgsodium key

If the primary uses pgsodium — Vault secrets or encrypted columns — the standby
needs the same encryption key, or that data is unreadable after a failover. The
key lives outside the data directory, so `pg_basebackup` does not bring it.

```sh
# on the primary
sh run.sh ha export-key > pgsodium.key.b64

# copy the file across, then on the standby
sh run.sh ha import-key < pgsodium.key.b64
docker compose -f standby.compose.yml restart db
```

### Tell the router about it

Back on the primary:

```sh
sh run.sh ha nodes add node-b <standby-address> 5434
sh run.sh ha router-reload
sh run.sh ha status
```

`ha status` shows each node's role and how far behind the standbys are.

Repeat for as many standbys as you want — the router health-checks all of them,
and reads are spread across the ones that are caught up.

---

## Failover

### Manual (default)

The agent notices the primary is gone and says so in its log, but changes
nothing. When you have confirmed the primary is really down:

```sh
sh run.sh ha promote            # on the standby server
```

### Automatic

Set on the standby:

```sh
HA_FAILOVER_MODE=auto
HA_WITNESS_URLS=http://<witness-address>:8008
```

The standby then promotes itself once the primary has been unreachable for
`HA_FAILURE_THRESHOLD` checks *and* a majority of witnesses agree they cannot
reach it either. In testing this takes about six seconds end to end, with the
router following immediately.

### Split-brain

Two nodes cannot safely decide a failover between themselves. If the *link*
between them breaks while both are alive, the standby cannot tell that apart
from a dead primary. Promote in that situation and you have two primaries taking
writes, with data that cannot be merged afterwards.

That is why automatic failover requires witnesses, and refuses to act without
them. A witness is any third machine that can see the primary independently —
it needs no database of its own:

```yaml
services:
  witness:
    build: ./ha
    restart: unless-stopped
    environment:
      HA_ROLE: witness
      HA_API_TOKEN: <the same token>
    ports:
      - 8008:8008
```

Put it somewhere with its own path to the primary — a third server, another
site. A witness on the same host as the standby proves nothing.

As a second line of defence, the router treats the first node in `HA_NODES` as
preferred and the rest as backups. If two nodes ever claim to be primary, traffic
stays with the original one instead of flapping between them.

---

## After a failover

The old primary is now stale. It cannot simply be started again — it would come
up as a second primary with diverging data.

Rejoin it as a standby, which discards its old contents:

```sh
# on the old primary, now becoming a standby
docker compose down
docker volume rm <project>_standby-data     # or the db data volume
# set HA_PRIMARY_HOST to the new primary, then
docker compose -f standby.compose.yml up -d
```

This is deliberately manual. It destroys whatever that node still held, which
may include writes that never reached the standby, and that is not a decision
to automate.

---

## The overview page

Every agent serves a status page on its own port:

```
http://<any-server>:8008/
```

It lists every node in the group with its role, replication state and lag,
refreshes itself, and offers a promote button for standbys. Paste the API token
into the field at the top to enable promoting; it is kept in the browser tab
only.

Two things it deliberately calls out: **no node is primary** (writes are
failing) and **two nodes are primary** (a split brain — writes are going to both
and will diverge). The second will not resolve itself; stop one node and rebuild
it as a standby.

The page is served by the agent itself rather than built into Studio. That keeps
it working when the database it reports on is down, keeps it reachable on every
node including a standby whose primary has vanished, and keeps this feature out
of the Studio codebase — which matters for a fork that regularly pulls thousands
of upstream commits.

Because a browser cannot call an agent on another server directly, promotions
from the page are relayed by the agent you have open. It only relays to nodes in
its own `HA_PEERS` list.

Keep port 8008 off the public internet. The page needs no token to *read*
status, on purpose: the router polls the same endpoints, and a status page that
cannot load during an incident is useless.

## Operating

| Command | Does |
| --- | --- |
| `sh run.sh ha status` | Role and lag of every node |
| `sh run.sh ha promote [url]` | Promote a standby, with a confirmation prompt |
| `sh run.sh ha nodes` | Show the router's node list |
| `sh run.sh ha nodes add …` | Register another node |
| `sh run.sh ha router-reload` | Apply a changed node list |
| `sh run.sh ha export-key` / `import-key` | Move the pgsodium key |

The router's own status page is on port 8404.

Each agent also answers directly:

| Endpoint | |
| --- | --- |
| `GET /health` | Full status as JSON |
| `GET /primary` | 200 if this node is the primary |
| `GET /replica` | 200 if this node is a standby fit to serve reads |
| `POST /promote` | Promote (needs the API token) |

---

## Reading from standbys

The router exposes a read-only port (`HA_ROUTER_READ_PORT`, default `5433`)
balanced across standbys that are streaming and caught up. Set
`HA_MAX_LAG_BYTES` to withdraw a standby that falls too far behind.

Only send queries there that tolerate slightly stale data — a standby is by
definition a moment behind, and writes will be rejected.

---

## Settings

| Variable | Default | |
| --- | --- | --- |
| `HA_NODE_NAME` | hostname | Name of this node |
| `HA_NODES` | — | Router's node list: `name=host:pgport:agentport,…` |
| `HA_API_TOKEN` | — | Shared secret; every node and witness needs the same one |
| `HA_FAILOVER_MODE` | `manual` | `manual`, `auto`, or `off` |
| `HA_WITNESS_URLS` | — | Comma-separated witness agents; required for `auto` |
| `HA_CHECK_INTERVAL` | `5` | Seconds between checks of the primary |
| `HA_FAILURE_THRESHOLD` | `3` | Consecutive misses before acting |
| `HA_MAX_LAG_BYTES` | `0` | Withdraw a standby from reads beyond this lag; 0 = no limit |
| `HA_DB_REPLICATION_PORT` | `5434` | Published Postgres port for replication |
| `HA_AGENT_PORT` | `8008` | Published agent port |
| `HA_REPLICATION_CIDR` | `0.0.0.0/0` | Which addresses may open replication connections |

---

## What this does not do

- **No multi-master.** One primary accepts writes at a time.
- **No automatic rejoin** of a failed primary — see above for why.
- **No failover for the API layer.** This covers the database. If server A is
  gone entirely, something still has to point clients at server B's Kong; that
  is a DNS or load-balancer job outside this stack.
- **Synchronous replication is not enabled.** The default is asynchronous, so a
  failover can lose the last few transactions that had not reached the standby.
  Setting `synchronous_standby_names` on the primary trades write latency for
  not losing them.

---

## Testing it

`tests/test-ha-failover.sh` brings up two nodes, a witness and a router in an
isolated project, and checks the whole path: the clone, continuous replication,
the roles each agent reports, the router's read/write split, an automatic
promotion after the primary is killed, and that no committed row is lost.

```sh
cd docker/
sh tests/test-ha-failover.sh

# against an image you already have locally
HA_TEST_IMAGE=supabase/postgres:15.8.1.048 sh tests/test-ha-failover.sh

# check that manual mode refuses to promote on its own
HA_TEST_FAILOVER_MODE=manual sh tests/test-ha-failover.sh
```

The agent's own logic has unit tests, including the cases where promotion must
be refused:

```sh
python3 -m unittest discover -s ha -v
```
