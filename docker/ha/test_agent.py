#!/usr/bin/env python3
"""
Tests for the HA agent's decision logic.

The focus is on the decisions that can lose data if they are wrong: when a
standby is allowed to serve reads, and — above all — when it is allowed to
promote itself. Postgres access is stubbed out; these are pure logic tests.

Run with:  python3 -m unittest discover -s docker/ha -v
"""

import os
import unittest
from unittest import mock

import agent


def make_config(**overrides) -> agent.Config:
    """Builds a Config from a clean environment plus the given overrides."""
    env = {
        "HA_NODE_NAME": "node-test",
        "HA_API_TOKEN": "test-token",
        "HA_PG_PASSWORD": "pw",
        **overrides,
    }
    with mock.patch.dict(os.environ, env, clear=True):
        return agent.Config()


class ConfigValidation(unittest.TestCase):
    def test_accepts_a_sane_configuration(self):
        self.assertEqual(make_config().validate(), [])

    def test_rejects_a_missing_token(self):
        problems = make_config(HA_API_TOKEN="").validate()
        self.assertTrue(any("HA_API_TOKEN" in p for p in problems), problems)

    def test_rejects_an_unknown_failover_mode(self):
        problems = make_config(HA_FAILOVER_MODE="whenever").validate()
        self.assertTrue(any("HA_FAILOVER_MODE" in p for p in problems), problems)

    def test_rejects_an_unknown_role(self):
        problems = make_config(HA_ROLE="something").validate()
        self.assertTrue(any("HA_ROLE" in p for p in problems), problems)

    def test_upstream_makes_it_a_standby(self):
        self.assertFalse(make_config().is_standby_config)
        self.assertTrue(make_config(HA_PRIMARY_HOST="10.0.0.1").is_standby_config)

    def test_a_witness_never_counts_as_a_standby(self):
        cfg = make_config(HA_ROLE="witness", HA_PRIMARY_HOST="10.0.0.1")
        self.assertTrue(cfg.is_witness)
        # A witness must not run a failover supervisor: it has no data to
        # promote, and doing so would put a second primary in the group.
        self.assertFalse(cfg.is_standby_config)


class ReplicaServability(unittest.TestCase):
    def test_a_streaming_standby_serves_reads(self):
        cfg = make_config()
        status = {"role": "standby", "streaming": True, "lag_bytes": 100}
        self.assertTrue(agent.replica_is_servable(cfg, status))

    def test_a_primary_is_not_a_read_replica(self):
        cfg = make_config()
        status = {"role": "primary", "streaming": True, "lag_bytes": 0}
        self.assertFalse(agent.replica_is_servable(cfg, status))

    def test_a_disconnected_standby_is_withdrawn(self):
        cfg = make_config()
        status = {"role": "standby", "streaming": False, "lag_bytes": 0}
        self.assertFalse(agent.replica_is_servable(cfg, status))

    def test_a_standby_beyond_the_lag_limit_is_withdrawn(self):
        cfg = make_config(HA_MAX_LAG_BYTES="1000")
        stale = {"role": "standby", "streaming": True, "lag_bytes": 5000}
        fresh = {"role": "standby", "streaming": True, "lag_bytes": 500}
        self.assertFalse(agent.replica_is_servable(cfg, stale))
        self.assertTrue(agent.replica_is_servable(cfg, fresh))

    def test_lag_limit_of_zero_means_unlimited(self):
        cfg = make_config(HA_MAX_LAG_BYTES="0")
        status = {"role": "standby", "streaming": True, "lag_bytes": 10**9}
        self.assertTrue(agent.replica_is_servable(cfg, status))


class WitnessQuorum(unittest.TestCase):
    """A standby may only promote when others also cannot see the primary."""

    def build(self, witnesses, answers):
        cfg = make_config(
            HA_PRIMARY_HOST="10.0.0.1",
            HA_FAILOVER_MODE="auto",
            HA_WITNESS_URLS=",".join(witnesses),
        )
        sup = agent.Supervisor(cfg, mock.Mock())
        sup._ask_witness = mock.Mock(side_effect=answers)
        return sup

    def test_all_witnesses_agree_the_primary_is_gone(self):
        sup = self.build(["http://w1", "http://w2"], [False, False])
        self.assertTrue(sup.witnesses_confirm_primary_down())

    def test_a_witness_that_still_sees_the_primary_blocks_promotion(self):
        # This is the network-split case: we cannot reach the primary, but
        # someone else can, so the primary is alive and still taking writes.
        sup = self.build(["http://w1", "http://w2"], [True, True])
        self.assertFalse(sup.witnesses_confirm_primary_down())

    def test_a_tie_is_not_a_majority(self):
        sup = self.build(["http://w1", "http://w2"], [False, True])
        self.assertFalse(sup.witnesses_confirm_primary_down())

    def test_a_majority_of_three_decides(self):
        sup = self.build(["http://w1", "http://w2", "http://w3"], [False, False, True])
        self.assertTrue(sup.witnesses_confirm_primary_down())

    def test_silence_is_not_agreement(self):
        # No witness answered. We know nothing, so we must not promote.
        sup = self.build(["http://w1", "http://w2"], [None, None])
        self.assertFalse(sup.witnesses_confirm_primary_down())

    def test_unreachable_witnesses_are_ignored_not_counted(self):
        # One witness is down, the other says the primary is gone: that is a
        # majority of the witnesses that actually answered.
        sup = self.build(["http://w1", "http://w2"], [None, False])
        self.assertTrue(sup.witnesses_confirm_primary_down())


class PromotionPolicy(unittest.TestCase):
    def build(self, **overrides):
        cfg = make_config(HA_PRIMARY_HOST="10.0.0.1", **overrides)
        node = mock.Mock()
        node.in_recovery.return_value = True
        sup = agent.Supervisor(cfg, node)
        sup.do_promote = mock.Mock(return_value=True)
        return sup

    def test_manual_mode_never_promotes_on_its_own(self):
        sup = self.build(HA_FAILOVER_MODE="manual")
        sup.on_primary_lost()
        sup.do_promote.assert_not_called()

    def test_auto_mode_without_witnesses_refuses_to_promote(self):
        # Two nodes alone cannot tell a dead primary from a broken link.
        sup = self.build(HA_FAILOVER_MODE="auto", HA_WITNESS_URLS="")
        sup.on_primary_lost()
        sup.do_promote.assert_not_called()
        self.assertIn("no witnesses", sup.last_error)

    def test_auto_mode_promotes_once_witnesses_agree(self):
        sup = self.build(HA_FAILOVER_MODE="auto", HA_WITNESS_URLS="http://w1")
        sup.witnesses_confirm_primary_down = mock.Mock(return_value=True)
        sup.on_primary_lost()
        sup.do_promote.assert_called_once()

    def test_auto_mode_stands_down_when_witnesses_disagree(self):
        sup = self.build(HA_FAILOVER_MODE="auto", HA_WITNESS_URLS="http://w1")
        sup.witnesses_confirm_primary_down = mock.Mock(return_value=False)
        sup.on_primary_lost()
        sup.do_promote.assert_not_called()
        self.assertIn("split", sup.last_error)


class FailureCounting(unittest.TestCase):
    def build(self, reachable):
        cfg = make_config(HA_PRIMARY_HOST="10.0.0.1", HA_FAILURE_THRESHOLD="3")
        node = mock.Mock()
        node.in_recovery.return_value = True
        sup = agent.Supervisor(cfg, node)
        sup.on_primary_lost = mock.Mock()
        self._reach = mock.patch.object(agent, "can_reach", return_value=reachable)
        self._reach.start()
        self.addCleanup(self._reach.stop)
        return sup

    def test_a_reachable_primary_clears_earlier_failures(self):
        sup = self.build(reachable=True)
        sup.consecutive_failures = 2
        sup.check_once()
        self.assertEqual(sup.consecutive_failures, 0)
        sup.on_primary_lost.assert_not_called()

    def test_failover_waits_for_the_threshold(self):
        sup = self.build(reachable=False)
        sup.check_once()
        sup.check_once()
        # Two misses out of three — a single blip must not trigger a failover.
        sup.on_primary_lost.assert_not_called()
        sup.check_once()
        sup.on_primary_lost.assert_called_once()

    def test_a_node_that_is_already_primary_stops_supervising(self):
        sup = self.build(reachable=False)
        sup.node.in_recovery.return_value = False
        sup.check_once()
        sup.on_primary_lost.assert_not_called()
        self.assertEqual(sup.consecutive_failures, 0)

    def test_a_local_outage_is_not_blamed_on_the_primary(self):
        sup = self.build(reachable=False)
        sup.node.in_recovery.side_effect = agent.PostgresError("connection refused")
        sup.check_once()
        sup.on_primary_lost.assert_not_called()
        self.assertEqual(sup.consecutive_failures, 0)
        self.assertIn("local node unreachable", sup.last_error)


class StatusParsing(unittest.TestCase):
    def test_standby_lag_is_parsed(self):
        cfg = make_config()
        node = agent.Node(cfg)
        node.query = mock.Mock(return_value="1.25|4096|0/3000148|1")
        self.assertEqual(
            node.standby_lag(),
            {
                "lag_seconds": 1.25,
                "lag_bytes": 4096,
                "receive_lsn": "0/3000148",
                "streaming": True,
            },
        )

    def test_a_standby_with_no_wal_receiver_is_not_streaming(self):
        cfg = make_config()
        node = agent.Node(cfg)
        node.query = mock.Mock(return_value="0|0||0")
        result = node.standby_lag()
        self.assertFalse(result["streaming"])
        self.assertIsNone(result["receive_lsn"])

    def test_connected_replicas_are_parsed(self):
        cfg = make_config()
        node = agent.Node(cfg)
        node.query = mock.Mock(
            return_value="10.0.0.2/32|streaming|async|0\n10.0.0.3/32|catchup|async|8192"
        )
        replicas = node.connected_replicas()
        self.assertEqual(len(replicas), 2)
        self.assertEqual(replicas[0]["client_addr"], "10.0.0.2/32")
        self.assertEqual(replicas[0]["state"], "streaming")
        self.assertEqual(replicas[1]["lag_bytes"], 8192)

    def test_a_primary_with_no_replicas_reports_none(self):
        cfg = make_config()
        node = agent.Node(cfg)
        node.query = mock.Mock(return_value="")
        self.assertEqual(node.connected_replicas(), [])


class StatusReport(unittest.TestCase):
    def test_an_unreachable_node_reports_down(self):
        cfg = make_config()
        node = mock.Mock()
        node.in_recovery.side_effect = agent.PostgresError("could not connect")
        status = agent.build_status(cfg, node, None)
        self.assertEqual(status["role"], "down")
        self.assertFalse(status["healthy"])

    def test_a_witness_reports_itself_without_a_database(self):
        cfg = make_config(HA_ROLE="witness")
        node = mock.Mock()
        node.in_recovery.side_effect = AssertionError("must not touch Postgres")
        status = agent.build_status(cfg, node, None)
        self.assertEqual(status["role"], "witness")
        self.assertTrue(status["healthy"])
        # And it must never be routed to as a database.
        self.assertFalse(agent.replica_is_servable(cfg, status))


if __name__ == "__main__":
    unittest.main()
