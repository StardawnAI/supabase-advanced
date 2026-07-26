# Status

Running log for the high-availability work. Newest first.

## 2026-07-26 — Upstream sync automation, and a Studio page

### Keeping upstream updates from eating our work

`.github/workflows/sync-upstream.yml` merges `supabase/supabase` daily. Daily,
not weekly, so each merge stays small enough to resolve.

It never resolves a conflict on its own:

| Outcome          | What happens                                                                     |
| ---------------- | -------------------------------------------------------------------------------- |
| Clean merge      | Verifies our changes survived, then pushes                                       |
| Conflict         | Pushes a branch, opens a PR naming the colliding files, pushes nothing to master |
| Our work missing | Pushes nothing and fails loudly                                                  |

The third case is the point. `.github/fork/verify-intact.sh` reads
`.github/fork/protected-paths.txt` and asserts that every file we own still
exists **and** that every hook we placed inside an upstream file is still
there. Verified by removing the `run.sh` hook and confirming the check fails
(`MISSING MARKER docker/run.sh`), then restoring it: 32/32.

**No blanket "ours" merge strategy**, deliberately. Always preferring our
version of a contested file also discards whatever upstream fixed in it,
including security fixes, and does so silently. The guard makes losing our work
loud instead, which is the part that actually needed solving.

The strategy the guard enforces is: keep the fork's footprint inside upstream
files as small as possible. It is currently **four lines across four files**
(`run.sh`, `.gitattributes`, `.env.example`, `SettingsMenu.utils.tsx`).
Everything else lives in files upstream has never heard of, where a conflict is
impossible.

### Studio page — and what it costs

`/project/default/settings/standby-servers` shows every node, its role, its
replication state and its lag, and calls out the two states worth waking up
for: no primary, and two primaries.

Findings that shaped it:

- **Self-hosted Studio is a prebuilt image** (`docker/docker-compose.yml:17`
  pins `supabase/studio:2026.07.07-sha-a6a04f2`, no `build:`). No change under
  `apps/studio` reaches a running instance without building and publishing a
  custom image. Hence `docker-compose.studio-fork.yml` (an overlay, so the
  upstream image line is never edited) and `.github/workflows/build-studio-image.yml`.
- **Upstream already has a "High Availability" feature** of its own for
  platform projects (`hooks/misc/useHighAvailability.ts`). Ours was renamed to
  **Standby servers** to avoid both the confusion and a future collision if
  upstream adds a settings page under that name.
- **The page is read-only.** Self-hosted Studio has no auth of its own
  (`withAuth` is a no-op when `IS_PLATFORM` is false), so a promote button here
  would quietly change the authority to fail over a database from "knows the HA
  token" to "knows the dashboard password". Promotion stays on the agent page.
- **The page cannot be relied on during an outage**: `standby.compose.yml` runs
  no Studio, so it is served only from the primary host. It is a health view
  for normal operation; the agent's own page on port 8008 answers on every node
  and is the one for incidents.
- **`next build` does not typecheck** (`ignoreBuildErrors: true`), and the only
  typecheck workflow is PR-triggered on runner labels this fork does not have.
  A type error would ship as a successfully built image and a blank page. Run
  `pnpm --filter studio typecheck` by hand.

### Bug found in yesterday's work

`ha-agent` declared `depends_on: db: condition: service_healthy`. After a
reboot with a damaged database the agent would never start — the one component
able to report the outage would itself be missing. Removed on both the primary
overlay and the standby stack.

### Proof

- Studio unit tests: **10 passed** (`npx vitest run components/interfaces/Settings/StandbyServers`).
- Typecheck: **0 errors in the fork's own files.** The run reports 216
  pre-existing `ui-patterns/admonition` resolution errors across upstream files,
  an artefact of installing with `--node-linker=hoisted --ignore-scripts` to work
  around a Windows/OneDrive symlink failure; `Admonition` is exported normally
  (`packages/ui-patterns/src/admonition/index.tsx:1`).
- Fork guard: **32/32**, and verified to fail when a hook is removed.

### Deliberate deviation

No entry was added to `apps/studio/TANSTACK_MIGRATION.md`, though the Studio
conventions ask for one. That file tracks upstream's own migration progress;
adding a fork-only page to it would mean one more line inside an upstream file
— the exact thing the sync strategy minimises — for no functional gain. The
`routes/**` twin itself _was_ added, since without it the page would silently
vanish from a TanStack build.

## 2026-07-25 — HA feature complete on `feat/ha-replication`

### Fork brought up to date

The fork had **no commits of its own** and was 5039 behind upstream, last synced
2025-09-13. Fast-forwarded to upstream `2b27ed0ab1` (2026-07-24) and pushed.
Nothing could be lost — there was nothing local to lose. No fork-sync GitHub
Action exists in this repo; the sync was done from the console.

### What was built

Streaming replication with automatic failover for the self-hosted stack.

| Piece                                                       | File                               |
| ----------------------------------------------------------- | ---------------------------------- |
| Agent: roles, lag, promotion, witness quorum, overview page | `docker/ha/agent.py`               |
| Standby clone                                               | `docker/ha/bootstrap-standby.sh`   |
| Primary preparation                                         | `docker/ha/setup-primary.sh`       |
| Router config generator                                     | `docker/ha/render-haproxy.sh`      |
| CLI behind `run.sh ha`                                      | `docker/ha/ha-cli.sh`              |
| Primary overlay                                             | `docker/docker-compose.ha.yml`     |
| Standby stack (second server)                               | `docker/standby.compose.yml`       |
| Unit tests (37)                                             | `docker/ha/test_agent.py`          |
| End-to-end test                                             | `docker/tests/test-ha-failover.sh` |
| Documentation                                               | `docker/ha/README.md`              |

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
- Manual mode: **27/27 checks passed.** The standby did _not_ promote itself, a
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
