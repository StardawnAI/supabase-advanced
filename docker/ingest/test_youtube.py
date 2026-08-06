#!/usr/bin/env python3
"""
Unit tests for the YouTube side of the ingest layer.

Run:  python3 -m unittest discover -s docker/ingest

Nothing here talks to YouTube. What is tested is the logic that decides what to
do with YouTube's answers — which is where this can go wrong quietly: a refusal
mistaken for "no captions" retires a working route, and "no captions" mistaken
for a refusal walks the whole chain for a video that will never have any.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetcher  # noqa: E402
import worker  # noqa: E402


def player(status="OK", reason="", tracks=None):
    return {
        "playabilityStatus": {"status": status, "reason": reason},
        "captions": {
            "playerCaptionsTracklistRenderer": {"captionTracks": tracks or []}
        },
    }


TRACK_DE = {"languageCode": "de", "baseUrl": "https://x/de", "kind": "asr"}
TRACK_DE_MANUAL = {"languageCode": "de", "baseUrl": "https://x/de-manual"}
TRACK_EN = {"languageCode": "en", "baseUrl": "https://x/en"}


# --------------------------------------------------------------------------
# Telling a refusal from an absence
# --------------------------------------------------------------------------


class RefusalTest(unittest.TestCase):
    def test_login_required_is_a_refusal(self):
        self.assertTrue(fetcher.is_refusal(player("LOGIN_REQUIRED"), []))

    def test_sign_in_to_confirm_is_a_refusal(self):
        response = player("ERROR", "Sign in to confirm you're not a bot")
        self.assertTrue(fetcher.is_refusal(response, []))

    def test_unplayable_without_captions_is_treated_as_a_refusal(self):
        # This is what a blocked client looks like when it phrases itself as
        # unavailability. Reading it as "no captions" would retire a route that
        # works and stop the rotation that would have succeeded.
        self.assertTrue(fetcher.is_refusal(player("UNPLAYABLE"), []))

    def test_unplayable_with_captions_is_not_a_refusal(self):
        # It answered and handed over the tracks. Whatever it will not play,
        # the captions are right there.
        self.assertFalse(fetcher.is_refusal(player("UNPLAYABLE"), [TRACK_EN]))

    def test_a_normal_answer_is_not_a_refusal(self):
        self.assertFalse(fetcher.is_refusal(player("OK"), [TRACK_EN]))


class TrackChoiceTest(unittest.TestCase):
    def test_preferred_language_wins(self):
        chosen = fetcher.choose_track([TRACK_EN, TRACK_DE_MANUAL], "de")
        self.assertEqual(chosen["baseUrl"], "https://x/de-manual")

    def test_manual_captions_beat_automatic_ones_in_the_same_language(self):
        # Automatic captions carry no punctuation, and sentence-mode chunking
        # has nothing to cut on without it — every chunk would end mid-thought.
        chosen = fetcher.choose_track([TRACK_DE, TRACK_DE_MANUAL], "de")
        self.assertEqual(chosen["baseUrl"], "https://x/de-manual")

    def test_automatic_in_the_right_language_beats_manual_in_another(self):
        chosen = fetcher.choose_track([TRACK_EN, TRACK_DE], "de")
        self.assertEqual(chosen["languageCode"], "de")

    def test_without_a_preference_manual_still_wins(self):
        chosen = fetcher.choose_track([TRACK_DE, TRACK_EN], None)
        self.assertEqual(chosen["baseUrl"], "https://x/en")

    def test_falls_back_to_whatever_exists(self):
        chosen = fetcher.choose_track([TRACK_DE], "fr")
        self.assertEqual(chosen["languageCode"], "de")


# --------------------------------------------------------------------------
# Parsing
# --------------------------------------------------------------------------


TRANSCRIPT_XML = """<?xml version="1.0" encoding="utf-8"?>
<transcript>
  <text start="0.5" dur="2.0">Willkommen zur&amp;#39;ck</text>
  <text start="2.5" dur="1.5">Heute geht es um Versicherungen.</text>
  <text start="4.0" dur="0.5">   </text>
  <text start="4.5" dur="2.0">Ein Satz mit &amp;quot;Anf&amp;#252;hrungszeichen&amp;quot;.</text>
</transcript>"""


class ParseTranscriptTest(unittest.TestCase):
    def test_text_and_spans(self):
        text, segments = fetcher.parse_transcript(TRANSCRIPT_XML)
        self.assertIn("Versicherungen", text)
        self.assertEqual(len(segments), 3)
        self.assertEqual(segments[0]["start_ms"], 500)
        self.assertEqual(segments[0]["end_ms"], 2500)

    def test_entities_are_decoded(self):
        text, _ = fetcher.parse_transcript(TRANSCRIPT_XML)
        self.assertIn('"Anführungszeichen"', text)
        self.assertNotIn("&quot;", text)

    def test_blank_segments_are_dropped(self):
        _, segments = fetcher.parse_transcript(TRANSCRIPT_XML)
        self.assertTrue(all(s["text"].strip() for s in segments))

    def test_an_empty_track_is_reported_as_no_captions(self):
        with self.assertRaises(fetcher.NoCaptionsError):
            fetcher.parse_transcript("<transcript></transcript>")

    def test_garbage_is_reported_as_unparseable(self):
        with self.assertRaises(fetcher.TranscriptError):
            fetcher.parse_transcript("<not xml")


class Srv3ParseTest(unittest.TestCase):
    """The dialect real YouTube answered with on the first live run.

    The stub was built from the legacy `<transcript><text>` form; the very
    first video fetched from the real site came back as srv3 and the parser
    threw "contained no text". This payload is a shortened copy of that real
    response ("Me at the zoo", jNQXAC9IVRw).
    """

    SRV3 = (
        '<?xml version="1.0" encoding="utf-8" ?><timedtext format="3"> <body> '
        '<p t="1200" d="2160">All right, so here we are, in front of the elephants</p> '
        '<p t="5318" d="2656">the cool thing about these guys is that they have really...</p> '
        '<p t="7974" d="4642">really really long trunks</p> </body> </timedtext>'
    )

    def test_srv3_milliseconds_are_taken_as_they_are(self):
        text, segments = fetcher.parse_transcript(self.SRV3)
        self.assertEqual(len(segments), 3)
        self.assertEqual(segments[0]["start_ms"], 1200)
        self.assertEqual(segments[0]["end_ms"], 3360)
        self.assertIn("elephants", text)

    def test_srv3_word_level_children_are_joined(self):
        # Auto-generated tracks split a line into <s> children per word.
        payload = (
            '<timedtext format="3"><body>'
            '<p t="0" d="1000"><s>never</s><s> gonna</s><s> give</s></p>'
            "</body></timedtext>"
        )
        text, segments = fetcher.parse_transcript(payload)
        self.assertEqual(segments[0]["text"], "never gonna give")

    def test_the_legacy_dialect_still_parses(self):
        payload = (
            "<transcript>"
            '<text start="1.2" dur="2.16">All right, so here we are</text>'
            "</transcript>"
        )
        text, segments = fetcher.parse_transcript(payload)
        self.assertEqual(segments[0]["start_ms"], 1200)
        self.assertEqual(segments[0]["end_ms"], 3360)


class DurationTest(unittest.TestCase):
    def test_hours_minutes_seconds(self):
        self.assertEqual(fetcher.parse_duration("PT1H2M3S"), 3723)

    def test_minutes_only(self):
        self.assertEqual(fetcher.parse_duration("PT47M"), 2820)

    def test_seconds_only(self):
        self.assertEqual(fetcher.parse_duration("PT59S"), 59)

    def test_a_live_stream_with_no_duration(self):
        self.assertEqual(fetcher.parse_duration(""), 0)
        self.assertEqual(fetcher.parse_duration("P0D"), 0)


class TargetParsingTest(unittest.TestCase):
    def test_playlist_wins_over_the_video_inside_it(self):
        # This is the URL you get from clicking a video within a playlist.
        # Someone who pastes it means the playlist.
        kind, ident = fetcher.parse_target(
            "https://www.youtube.com/watch?v=dQw4w9WgXcQ&list=PLabcdefghijkl"
        )
        self.assertEqual((kind, ident), ("playlist", "PLabcdefghijkl"))

    def test_short_links_and_shorts(self):
        self.assertEqual(
            fetcher.parse_target("https://youtu.be/dQw4w9WgXcQ"),
            ("video", "dQw4w9WgXcQ"),
        )
        self.assertEqual(
            fetcher.parse_target("https://www.youtube.com/shorts/dQw4w9WgXcQ"),
            ("video", "dQw4w9WgXcQ"),
        )

    def test_bare_ids_are_told_apart_by_their_prefix(self):
        self.assertEqual(
            fetcher.parse_target("UC1234567890abcdefghij")[0], "channel"
        )
        self.assertEqual(fetcher.parse_target("PLabcdefghijkl")[0], "playlist")
        self.assertEqual(fetcher.parse_target("dQw4w9WgXcQ")[0], "video")

    def test_nonsense_is_refused_with_a_readable_message(self):
        with self.assertRaises(fetcher.TranscriptError) as ctx:
            fetcher.parse_target("my holiday videos")
        self.assertIn("cannot tell", str(ctx.exception))


# --------------------------------------------------------------------------
# Client rotation
# --------------------------------------------------------------------------


PROVIDER = {
    "key": "warp_innertube",
    "use_proxy": True,
    "clients": [
        {"clientName": "ANDROID", "clientVersion": "1", "userAgent": "a"},
        {"clientName": "WEB", "clientVersion": "2", "userAgent": "b"},
    ],
}


class RotationTest(unittest.TestCase):
    def setUp(self):
        self.key_patch = mock.patch.object(
            fetcher, "innertube_key", return_value="AIzaTest"
        )
        self.key_patch.start()
        self.addCleanup(self.key_patch.stop)

    def test_moves_to_the_next_client_when_one_is_refused(self):
        responses = [player("LOGIN_REQUIRED"), player("OK", tracks=[TRACK_EN])]
        with mock.patch.object(fetcher, "player_response", side_effect=responses):
            with mock.patch.object(fetcher.Http, "get", return_value=TRANSCRIPT_XML):
                result = fetcher.fetch_via(PROVIDER, "vid", "en", "socks5h://p:1080")
        self.assertEqual(result["client"], "WEB")
        self.assertIn("Versicherungen", result["text"])

    def test_every_client_refused_is_a_blocked_route(self):
        responses = [player("LOGIN_REQUIRED"), player("ERROR", "Sign in to confirm")]
        with mock.patch.object(fetcher, "player_response", side_effect=responses):
            with self.assertRaises(fetcher.BlockedError) as ctx:
                fetcher.fetch_via(PROVIDER, "vid", None, None)
        # The message has to name what was tried, or diagnosing a blocked route
        # means reading logs from five separate attempts.
        self.assertIn("ANDROID", str(ctx.exception))
        self.assertIn("WEB", str(ctx.exception))

    def test_a_video_without_captions_stops_the_chain_immediately(self):
        # Distinct from a refusal on purpose: no other route invents captions,
        # and walking the whole chain would spend proxy requests to learn
        # nothing — and mark healthy providers as failing on the way.
        with mock.patch.object(
            fetcher, "player_response", return_value=player("OK", tracks=[])
        ):
            with self.assertRaises(fetcher.NoCaptionsError):
                fetcher.fetch_via(PROVIDER, "vid", None, None)

    def test_the_first_working_client_is_used_and_the_rest_are_not_tried(self):
        calls = []

        def record(http, video_id, api_key, client, use_proxy):
            calls.append(client["clientName"])
            return player("OK", tracks=[TRACK_EN])

        with mock.patch.object(fetcher, "player_response", side_effect=record):
            with mock.patch.object(fetcher.Http, "get", return_value=TRANSCRIPT_XML):
                fetcher.fetch_via(PROVIDER, "vid", None, None)
        self.assertEqual(calls, ["ANDROID"])


class ProxyRoutingTest(unittest.TestCase):
    def test_the_watch_page_is_not_proxied(self):
        # Spending a proxy request on a page any visitor can load wastes the
        # scarce resource on the cheap step.
        seen = {}

        def capture(self, url, headers=None, use_proxy=True):
            seen[url] = use_proxy
            return '"INNERTUBE_API_KEY":"AIzaTest"'

        with mock.patch.object(fetcher.Http, "get", capture):
            fetcher.innertube_key(fetcher.Http(proxy="socks5h://p:1080"), "vid")
        self.assertFalse(list(seen.values())[0])

    def test_config_form_keeps_a_secret_out_of_the_argument_list(self):
        http = fetcher.Http()
        config = http._config_lines(
            "https://api/videos?key=SUPERSECRET", {}, use_proxy=False
        )
        # The URL travels on stdin. `ps` is readable by every account on the
        # machine, so an API key in argv is an API key on offer.
        self.assertIn("SUPERSECRET", config)
        self.assertIn('url = "https://api/videos?key=SUPERSECRET"', config)

    def test_config_quoting_survives_a_quote_in_the_value(self):
        http = fetcher.Http()
        config = http._config_lines("https://x/", {"X-Test": 'a"b'}, use_proxy=False)
        self.assertIn('\\"', config)


class DataApiCredentialTest(unittest.TestCase):
    """The Data API credential may be an API key or an OAuth access token.

    A "sign in with Google" flow produces only the token — there is no API key
    anywhere in that world, so the fetcher must speak both. The n8n workflow
    this port replaces authenticated exactly this way.
    """

    def capture(self):
        seen = {}

        def fake_get(self, url, headers=None, use_proxy=True):
            seen["url"] = url
            seen["headers"] = headers or {}
            return '{"items": []}'

        return seen, fake_get

    def test_an_api_key_travels_as_the_key_parameter(self):
        seen, fake_get = self.capture()
        with mock.patch.object(fetcher.Http, "get", fake_get):
            fetcher._api(fetcher.Http(), "videos", {"id": "x", "key": "AIzaFakeKey"})
        self.assertIn("key=AIzaFakeKey", seen["url"])
        self.assertNotIn("Authorization", seen["headers"])

    def test_an_oauth_token_travels_as_a_bearer_header(self):
        seen, fake_get = self.capture()
        with mock.patch.object(fetcher.Http, "get", fake_get):
            fetcher._api(fetcher.Http(), "videos", {"id": "x", "key": "ya29.a0FakeToken"})
        # The token must not land in the URL: URLs end up in logs and shell
        # histories, headers here travel via curl's stdin config.
        self.assertNotIn("ya29", seen["url"])
        self.assertEqual(seen["headers"]["Authorization"], "Bearer ya29.a0FakeToken")

    def test_a_prefixed_bearer_value_is_not_double_prefixed(self):
        seen, fake_get = self.capture()
        with mock.patch.object(fetcher.Http, "get", fake_get):
            fetcher._api(fetcher.Http(), "videos", {"id": "x", "key": "Bearer tok123"})
        self.assertEqual(seen["headers"]["Authorization"], "Bearer tok123")


# --------------------------------------------------------------------------
# Timed chunking
# --------------------------------------------------------------------------


def segments(count: int, words: int = 6) -> list[dict]:
    return [
        {
            "start_ms": i * 2000,
            "end_ms": i * 2000 + 1800,
            "text": " ".join([f"w{i}"] * words),
        }
        for i in range(count)
    ]


class TimedChunkTest(unittest.TestCase):
    def test_every_chunk_carries_the_span_it_covers(self):
        chunks = worker.chunk_timed(segments(10), {"size": 100, "overlap": 0})
        self.assertTrue(chunks)
        for chunk in chunks:
            self.assertIn("start_ms", chunk["span"])
            self.assertLess(chunk["span"]["start_ms"], chunk["span"]["end_ms"])

    def test_spans_advance_and_cover_the_whole_recording(self):
        source = segments(12)
        chunks = worker.chunk_timed(source, {"size": 120, "overlap": 0})
        self.assertEqual(chunks[0]["span"]["start_ms"], source[0]["start_ms"])
        self.assertEqual(chunks[-1]["span"]["end_ms"], source[-1]["end_ms"])
        starts = [c["span"]["start_ms"] for c in chunks]
        self.assertEqual(starts, sorted(starts))

    def test_overlap_repeats_the_tail_and_rewinds_the_span(self):
        chunks = worker.chunk_timed(segments(20), {"size": 120, "overlap": 60})
        self.assertGreater(len(chunks), 2)
        # An overlapping chunk starts before the previous one ended — that is
        # what makes a passage on a boundary findable from either side.
        self.assertLess(chunks[1]["span"]["start_ms"], chunks[0]["span"]["end_ms"])

    def test_blank_segments_do_not_produce_empty_chunks(self):
        noisy = [{"start_ms": 0, "end_ms": 1, "text": "  "}] + segments(3)
        chunks = worker.chunk_timed(noisy, {"size": 200, "overlap": 0})
        self.assertTrue(all(c["content"].strip() for c in chunks))

    def test_no_segments_means_no_chunks(self):
        self.assertEqual(worker.chunk_timed([], {"size": 100}), [])

    def test_overlap_at_or_above_size_cannot_loop(self):
        chunks = worker.chunk_timed(segments(30), {"size": 100, "overlap": 100})
        self.assertGreater(len(chunks), 1)
        self.assertLess(len(chunks), 200)


# --------------------------------------------------------------------------
# Credentials
# --------------------------------------------------------------------------


class CredentialTest(unittest.TestCase):
    def test_an_env_reference_is_resolved(self):
        with mock.patch.dict("os.environ", {"MY_KEY": "abc"}, clear=False):
            value = worker.Worker.credential({"credential_ref": "env:MY_KEY"})
        self.assertEqual(value, "abc")

    def test_a_missing_variable_says_which_one(self):
        with self.assertRaises(RuntimeError) as ctx:
            worker.Worker.credential({"credential_ref": "env:DEFINITELY_NOT_SET"})
        self.assertIn("DEFINITELY_NOT_SET", str(ctx.exception))

    def test_an_inline_secret_is_refused_rather_than_used(self):
        # The whole point of storing a reference is that the database never
        # holds the secret. Accepting one here would quietly undo that.
        with self.assertRaises(RuntimeError) as ctx:
            worker.Worker.credential({"credential_ref": "AIzaSyRealLookingKey"})
        self.assertIn("env:NAME", str(ctx.exception))


class SchemaShapeTest(unittest.TestCase):
    def test_the_breaker_needs_three_failures_not_one(self):
        # One failure is a video; three in a row is the route. Tripping on one
        # would retire working providers on any unavailable video.
        sql = (Path(__file__).parent / "sql" / "004_youtube.sql").read_text(
            encoding="utf-8"
        )
        self.assertIn("consecutive_failures + 1 >= 3", sql)

    def test_a_down_provider_leaves_the_chain(self):
        sql = (Path(__file__).parent / "sql" / "004_youtube.sql").read_text(
            encoding="utf-8"
        )
        self.assertIn("health <> 'down'", sql)

    def test_client_identities_live_in_the_database(self):
        # Their version numbers go stale, and updating them must not require
        # a release.
        sql = (Path(__file__).parent / "sql" / "004_youtube.sql").read_text(
            encoding="utf-8"
        )
        self.assertIn("clients", sql)
        for client in ("ANDROID", "WEB", "MWEB", "IOS", "TVHTML5"):
            self.assertIn(client, sql)

    def test_reprocessing_gets_a_distinct_idempotency_key(self):
        # Without the revision suffix a reprocess is refused as a duplicate of
        # the original ingestion, which is what made the generation machinery
        # unreachable in the first place.
        sql = (Path(__file__).parent / "sql" / "003_operations.sql").read_text(
            encoding="utf-8"
        )
        self.assertIn("nextval('ingest.reprocess_revision')", sql)

    def test_stalled_jobs_are_returned_to_the_queue(self):
        sql = (Path(__file__).parent / "sql" / "003_operations.sql").read_text(
            encoding="utf-8"
        )
        self.assertIn("requeue_stalled", sql)
        self.assertIn("status = 'running'", sql)


if __name__ == "__main__":
    unittest.main(verbosity=2)
