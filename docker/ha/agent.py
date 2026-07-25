#!/usr/bin/env python3
"""
Supabase Advanced — HA agent.

One agent runs as a sidecar next to every Postgres node in an HA group. It:

  * answers the health probes the router uses to locate the current primary,
  * reports replication state and lag,
  * on a standby, promotes itself when the primary is gone.

Only the Python standard library and the `psql` client are required, so the
image stays small and there is no dependency tree to keep patched.

Endpoints
  GET  /primary    200 when this node is the primary   (router write backend)
  GET  /replica    200 when this node is a healthy standby (router read backend)
  GET  /health     200 always, JSON status — for humans and uptime checks
  GET  /status     same JSON as /health
  GET  /can-reach  witness probe: can *this* node see host:port?  (auth)
  POST /promote    promote this standby to primary                (auth)

Authenticated endpoints expect `Authorization: Bearer $HA_API_TOKEN`.
"""

from __future__ import annotations

import hmac
import json
import logging
import os
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOG = logging.getLogger("ha-agent")


class PostgresError(RuntimeError):
    """psql could not run the statement (node down, auth failure, ...)."""


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------


class Config:
    def __init__(self) -> None:
        env = os.environ.get
        self.node_name = env("HA_NODE_NAME") or socket.gethostname()
        self.port = int(env("HA_PORT", "8008"))
        self.token = env("HA_API_TOKEN", "")
        # "node"    — supervises a Postgres node (primary or standby)
        # "witness" — has no database of its own and only answers /can-reach,
        #             giving standbys an independent view of the primary
        self.role_mode = env("HA_ROLE", "node").strip().lower()

        # The Postgres node this agent is responsible for.
        self.pg_host = env("HA_PG_HOST", "db")
        self.pg_port = int(env("HA_PG_PORT", "5432"))
        self.pg_user = env("HA_PG_USER", "postgres")
        self.pg_password = env("HA_PG_PASSWORD", "")
        self.pg_database = env("HA_PG_DATABASE", "postgres")

        # Upstream primary. Set on standby nodes only; its presence is what
        # makes this agent run the failover monitor.
        self.primary_host = env("HA_PRIMARY_HOST", "")
        self.primary_port = int(env("HA_PRIMARY_PORT", "5432"))

        self.failover_mode = env("HA_FAILOVER_MODE", "manual").strip().lower()
        self.witness_urls = [
            u.strip().rstrip("/")
            for u in env("HA_WITNESS_URLS", "").split(",")
            if u.strip()
        ]
        self.check_interval = float(env("HA_CHECK_INTERVAL", "5"))
        self.failure_threshold = int(env("HA_FAILURE_THRESHOLD", "3"))
        self.connect_timeout = int(env("HA_CONNECT_TIMEOUT", "3"))
        # Standbys lagging more than this stop advertising /replica.
        # 0 disables the check.
        self.max_lag_bytes = int(env("HA_MAX_LAG_BYTES", "0"))

    @property
    def is_witness(self) -> bool:
        return self.role_mode == "witness"

    @property
    def is_standby_config(self) -> bool:
        return bool(self.primary_host) and not self.is_witness

    def validate(self) -> list[str]:
        """Returns a list of fatal configuration problems."""
        problems = []
        if self.role_mode not in ("node", "witness"):
            problems.append(f"HA_ROLE must be node or witness (got {self.role_mode!r})")
        if self.failover_mode not in ("manual", "auto", "off"):
            problems.append(
                f"HA_FAILOVER_MODE must be manual, auto or off (got {self.failover_mode!r})"
            )
        if not self.token:
            problems.append("HA_API_TOKEN must be set — /promote would be unauthenticated")
        if self.failure_threshold < 1:
            problems.append("HA_FAILURE_THRESHOLD must be >= 1")
        return problems


# --------------------------------------------------------------------------
# Postgres access
# --------------------------------------------------------------------------


def psql(
    sql: str,
    *,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    timeout: int,
) -> str:
    """Runs one statement and returns its output, unaligned and untitled."""
    cmd = [
        "psql", "-X", "-q", "-A", "-t",
        "-h", host, "-p", str(port), "-U", user, "-d", database,
        "-v", "ON_ERROR_STOP=1",
        "-c", sql,
    ]
    env = dict(os.environ, PGPASSWORD=password, PGCONNECT_TIMEOUT=str(timeout))
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, env=env, timeout=timeout + 5
        )
    except subprocess.TimeoutExpired as exc:
        raise PostgresError(f"psql timed out after {timeout + 5}s") from exc
    if proc.returncode != 0:
        raise PostgresError(proc.stderr.strip() or f"psql exited {proc.returncode}")
    return proc.stdout.strip()


class Node:
    """The local Postgres node."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg

    def query(self, sql: str) -> str:
        return psql(
            sql,
            host=self.cfg.pg_host,
            port=self.cfg.pg_port,
            user=self.cfg.pg_user,
            password=self.cfg.pg_password,
            database=self.cfg.pg_database,
            timeout=self.cfg.connect_timeout,
        )

    def in_recovery(self) -> bool:
        return self.query("SELECT pg_is_in_recovery()") == "t"

    def promote(self) -> bool:
        """Promotes this standby. Returns True once it is out of recovery."""
        self.query("SELECT pg_promote(wait := true, wait_seconds := 60)")
        return not self.in_recovery()

    def standby_lag(self) -> dict:
        """Replay lag of this standby against the WAL it has received."""
        row = self.query(
            "SELECT COALESCE(EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())), 0), "
            "COALESCE(pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn()), 0), "
            "COALESCE(pg_last_wal_receive_lsn()::text, ''), "
            "(SELECT count(*) FROM pg_stat_wal_receiver)"
        )
        seconds, lag_bytes, receive_lsn, receivers = (row.split("|") + ["", "", "", ""])[:4]
        return {
            "lag_seconds": round(float(seconds or 0), 3),
            "lag_bytes": int(float(lag_bytes or 0)),
            "receive_lsn": receive_lsn or None,
            "streaming": int(receivers or 0) > 0,
        }

    def connected_replicas(self) -> list[dict]:
        """Standbys currently streaming from this primary."""
        out = self.query(
            "SELECT COALESCE(client_addr::text, 'local'), state, sync_state, "
            "COALESCE(pg_wal_lsn_diff(sent_lsn, replay_lsn), 0) "
            "FROM pg_stat_replication"
        )
        replicas = []
        for line in filter(None, out.splitlines()):
            addr, state, sync_state, lag = (line.split("|") + ["", "", "", ""])[:4]
            replicas.append(
                {
                    "client_addr": addr,
                    "state": state,
                    "sync_state": sync_state,
                    "lag_bytes": int(float(lag or 0)),
                }
            )
        return replicas


def can_reach(host: str, port: int, user: str, password: str, timeout: int) -> bool:
    """True when host:port accepts a Postgres connection.

    A rejected login still proves the server is alive, so only connection-level
    failures count as unreachable.
    """
    cmd = ["pg_isready", "-h", host, "-p", str(port), "-U", user, "-t", str(timeout)]
    env = dict(os.environ, PGPASSWORD=password)
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, env=env, timeout=timeout + 5
        )
    except subprocess.TimeoutExpired:
        return False
    return proc.returncode == 0


# --------------------------------------------------------------------------
# Failover supervisor
# --------------------------------------------------------------------------


class Supervisor:
    """Watches the upstream primary and promotes this standby when it is gone.

    Auto-promotion deliberately requires witnesses. With only two nodes a
    standby cannot tell "primary is dead" from "I am cut off from the primary",
    and promoting during a network split gives two primaries with diverging
    data. A witness is any other agent that has an independent view of the
    primary; promotion needs a majority of them to agree it is unreachable.
    """

    def __init__(self, cfg: Config, node: Node) -> None:
        self.cfg = cfg
        self.node = node
        self.lock = threading.Lock()
        self.consecutive_failures = 0
        self.last_error: str | None = None
        self.promoted_at: float | None = None
        self.last_check: float | None = None

    # -- witness protocol --------------------------------------------------

    def _ask_witness(self, url: str) -> bool | None:
        """Asks a witness whether it can see the primary. None = no answer."""
        query = urllib.parse.urlencode(
            {"host": self.cfg.primary_host, "port": self.cfg.primary_port}
        )
        req = urllib.request.Request(
            f"{url}/can-reach?{query}",
            headers={"Authorization": f"Bearer {self.cfg.token}"},
        )
        try:
            with urllib.request.urlopen(req, timeout=self.cfg.connect_timeout + 2) as resp:
                return bool(json.load(resp).get("reachable"))
        except (urllib.error.URLError, OSError, ValueError, json.JSONDecodeError) as exc:
            LOG.warning("witness %s did not answer: %s", url, exc)
            return None

    def witnesses_confirm_primary_down(self) -> bool:
        """True when a majority of reachable witnesses also cannot see it."""
        answers = [self._ask_witness(url) for url in self.cfg.witness_urls]
        heard = [a for a in answers if a is not None]
        if not heard:
            LOG.error("no witness answered — refusing to promote")
            return False
        down_votes = sum(1 for a in heard if a is False)
        LOG.info(
            "witness quorum: %d/%d say the primary is unreachable", down_votes, len(heard)
        )
        return down_votes > len(heard) / 2

    # -- main loop ---------------------------------------------------------

    def check_once(self) -> None:
        self.last_check = time.time()

        try:
            if not self.node.in_recovery():
                # Already a primary — nothing left to supervise.
                self.consecutive_failures = 0
                return
        except PostgresError as exc:
            # Our own node is unreachable; that is not the primary's fault.
            self.last_error = f"local node unreachable: {exc}"
            LOG.warning("%s", self.last_error)
            return

        reachable = can_reach(
            self.cfg.primary_host,
            self.cfg.primary_port,
            self.cfg.pg_user,
            self.cfg.pg_password,
            self.cfg.connect_timeout,
        )
        if reachable:
            if self.consecutive_failures:
                LOG.info("primary is reachable again")
            self.consecutive_failures = 0
            return

        self.consecutive_failures += 1
        LOG.warning(
            "primary %s:%s unreachable (%d/%d)",
            self.cfg.primary_host,
            self.cfg.primary_port,
            self.consecutive_failures,
            self.cfg.failure_threshold,
        )
        if self.consecutive_failures < self.cfg.failure_threshold:
            return

        self.on_primary_lost()

    def on_primary_lost(self) -> None:
        mode = self.cfg.failover_mode
        if mode != "auto":
            LOG.error(
                "PRIMARY DOWN — failover mode is %r, not promoting. "
                "Promote manually with: run.sh ha promote",
                mode,
            )
            self.last_error = "primary down, waiting for manual promotion"
            return

        if not self.cfg.witness_urls:
            LOG.error(
                "PRIMARY DOWN — auto failover needs HA_WITNESS_URLS to rule out a "
                "network split. Not promoting."
            )
            self.last_error = "primary down, auto failover blocked: no witnesses"
            return

        if not self.witnesses_confirm_primary_down():
            LOG.error(
                "PRIMARY DOWN from here, but witnesses still see it — this looks "
                "like a network split. Not promoting."
            )
            self.last_error = "primary unreachable from this node only (suspected split)"
            return

        LOG.warning("promoting this node to primary")
        try:
            self.do_promote()
        except PostgresError as exc:
            self.last_error = f"promotion failed: {exc}"
            LOG.error("%s", self.last_error)

    def do_promote(self) -> bool:
        """Promotes the node. Safe to call concurrently."""
        with self.lock:
            if not self.node.in_recovery():
                return True
            promoted = self.node.promote()
            if promoted:
                self.promoted_at = time.time()
                self.consecutive_failures = 0
                self.last_error = None
                LOG.warning("this node is now the PRIMARY")
            return promoted

    def run(self) -> None:
        while True:
            try:
                self.check_once()
            except Exception:  # keep the supervisor alive whatever happens
                LOG.exception("supervisor check failed")
            time.sleep(self.cfg.check_interval)


# --------------------------------------------------------------------------
# Status
# --------------------------------------------------------------------------


def build_status(cfg: Config, node: Node, sup: Supervisor | None) -> dict:
    status = {
        "node": cfg.node_name,
        "role": "unknown",
        "healthy": False,
        "failover_mode": cfg.failover_mode,
        "upstream": (
            f"{cfg.primary_host}:{cfg.primary_port}" if cfg.is_standby_config else None
        ),
        "witnesses": cfg.witness_urls,
    }
    if cfg.is_witness:
        # A witness has no database. It is healthy as long as it is answering,
        # and it never claims to be primary or replica.
        status["role"] = "witness"
        status["healthy"] = True
        return status

    try:
        in_recovery = node.in_recovery()
    except PostgresError as exc:
        status["role"] = "down"
        status["error"] = str(exc)
        return status

    status["healthy"] = True
    status["role"] = "standby" if in_recovery else "primary"
    try:
        if in_recovery:
            status.update(node.standby_lag())
        else:
            status["replicas"] = node.connected_replicas()
    except PostgresError as exc:
        status["error"] = str(exc)

    if sup is not None:
        status["consecutive_failures"] = sup.consecutive_failures
        status["last_check"] = sup.last_check
        status["promoted_at"] = sup.promoted_at
        if sup.last_error:
            status["last_error"] = sup.last_error
    return status


def replica_is_servable(cfg: Config, status: dict) -> bool:
    """Whether a standby is fresh enough to serve reads."""
    if status.get("role") != "standby":
        return False
    if not status.get("streaming"):
        return False
    if cfg.max_lag_bytes and status.get("lag_bytes", 0) > cfg.max_lag_bytes:
        return False
    return True


# --------------------------------------------------------------------------
# HTTP interface
# --------------------------------------------------------------------------


class Handler(BaseHTTPRequestHandler):
    server_version = "supabase-ha-agent"
    cfg: Config
    node: Node
    supervisor: Supervisor | None

    def log_message(self, fmt: str, *args) -> None:  # quieter default logging
        LOG.debug("%s - %s", self.address_string(), fmt % args)

    # -- helpers -----------------------------------------------------------

    def _send(self, code: int, payload: dict) -> None:
        body = json.dumps(payload, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _authorized(self) -> bool:
        expected = f"Bearer {self.cfg.token}"
        provided = self.headers.get("Authorization", "")
        # Constant-time compare to avoid leaking the token byte by byte.
        if not self.cfg.token or not hmac.compare_digest(provided, expected):
            self._send(401, {"error": "unauthorized"})
            return False
        return True

    # -- routes ------------------------------------------------------------

    def do_GET(self) -> None:  # noqa: N802 (stdlib naming)
        route = urllib.parse.urlparse(self.path)
        path = route.path.rstrip("/") or "/"
        status = build_status(self.cfg, self.node, self.supervisor)

        if path in ("/health", "/status", "/"):
            self._send(200, status)
        elif path == "/primary":
            ok = status.get("role") == "primary"
            self._send(200 if ok else 503, status)
        elif path == "/replica":
            ok = replica_is_servable(self.cfg, status)
            self._send(200 if ok else 503, status)
        elif path == "/can-reach":
            self._handle_can_reach(route.query)
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        path = urllib.parse.urlparse(self.path).path.rstrip("/") or "/"
        if path != "/promote":
            self._send(404, {"error": "not found"})
            return
        if not self._authorized():
            return
        if self.supervisor is None:
            self._send(409, {"error": "this node is not configured as a standby"})
            return
        try:
            promoted = self.supervisor.do_promote()
        except PostgresError as exc:
            self._send(500, {"error": str(exc)})
            return
        self._send(
            200 if promoted else 500,
            {"promoted": promoted, **build_status(self.cfg, self.node, self.supervisor)},
        )

    def _handle_can_reach(self, query: str) -> None:
        """Witness probe — reports whether this node can see another node."""
        if not self._authorized():
            return
        params = urllib.parse.parse_qs(query)
        host = (params.get("host") or [""])[0]
        port_raw = (params.get("port") or ["5432"])[0]
        if not host:
            self._send(400, {"error": "host parameter required"})
            return
        try:
            port = int(port_raw)
        except ValueError:
            self._send(400, {"error": "port must be a number"})
            return
        reachable = can_reach(
            host, port, self.cfg.pg_user, self.cfg.pg_password, self.cfg.connect_timeout
        )
        self._send(200, {"host": host, "port": port, "reachable": reachable})


# --------------------------------------------------------------------------


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("HA_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    cfg = Config()
    problems = cfg.validate()
    if problems:
        for problem in problems:
            LOG.error("config: %s", problem)
        raise SystemExit(1)

    node = Node(cfg)
    supervisor = None
    if cfg.is_standby_config and cfg.failover_mode != "off":
        supervisor = Supervisor(cfg, node)
        threading.Thread(target=supervisor.run, daemon=True, name="supervisor").start()
        LOG.info(
            "supervising primary %s:%s (mode=%s, witnesses=%d)",
            cfg.primary_host,
            cfg.primary_port,
            cfg.failover_mode,
            len(cfg.witness_urls),
        )

    Handler.cfg = cfg
    Handler.node = node
    Handler.supervisor = supervisor

    server = ThreadingHTTPServer(("0.0.0.0", cfg.port), Handler)
    LOG.info("agent %s listening on :%d", cfg.node_name, cfg.port)
    server.serve_forever()


if __name__ == "__main__":
    main()
