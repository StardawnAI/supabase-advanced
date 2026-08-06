#!/usr/bin/env python3
"""
A stand-in for YouTube, for the end-to-end test.

The point is not to fake the result — it is to make the real request chain run.
curl really connects, the client rotation really rotates, the caption file is
really downloaded and parsed. Only the far end is ours, so the test does not
depend on YouTube being reachable, unblocked, or in a particular mood.

It reproduces the two behaviours the fetcher exists to handle:

  * ANDROID is refused with LOGIN_REQUIRED — exactly what a datacenter address
    gets in production — so the rotation has to move on to WEB.
  * one video answers with no caption tracks at all, which must stop the chain
    rather than mark the route as broken.
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

API_KEY = "AIzaSyStubKeyForTesting"

TRANSCRIPTS = {
    "vid00000001": [
        (0.5, 2.0, "Willkommen zu dieser Folge."),
        (2.5, 2.5, "Heute sprechen wir über Turbinenlager und ihre Wartung."),
        (5.0, 3.0, "Das Lager wurde am Dienstag getauscht."),
        (8.0, 2.0, "Danach waren die Schwingungswerte wieder normal."),
    ],
    "vid00000002": [
        (0.0, 3.0, "Ein zweites Video über Versicherungen."),
        (3.0, 3.0, "Die Police deckt Elementarschäden ab."),
    ],
    # Deliberately has no captions.
    "vid00000003": None,
}

PLAYLIST = ["vid00000001", "vid00000002", "vid00000003"]


def transcript_xml(entries) -> str:
    rows = "".join(
        f'<text start="{start}" dur="{dur}">{text}</text>'
        for start, dur, text in entries
    )
    return f'<?xml version="1.0" encoding="utf-8"?><transcript>{rows}</transcript>'


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        route = urlparse(self.path)
        params = parse_qs(route.query)

        if route.path == "/watch":
            self._send(
                200,
                f'<html><script>{{"INNERTUBE_API_KEY":"{API_KEY}",'
                f'"other":"noise"}}</script></html>',
                "text/html",
            )
        elif route.path == "/captions":
            video_id = (params.get("v") or [""])[0]
            entries = TRANSCRIPTS.get(video_id)
            self._send(200, transcript_xml(entries or []), "text/xml")
        elif route.path == "/youtube/v3/videos":
            video_id = (params.get("id") or [""])[0]
            self._json({
                "items": [{
                    "id": video_id,
                    "snippet": {
                        "title": f"Test video {video_id}",
                        "description": "A description",
                        "channelId": "UCstub0000000000000000",
                        "channelTitle": "Stub Channel",
                        "publishedAt": "2026-01-15T10:00:00Z",
                    },
                    "contentDetails": {"duration": "PT12M34S"},
                    "statistics": {"viewCount": "1234"},
                }]
            })
        elif route.path == "/youtube/v3/playlistItems":
            self._json({
                "items": [
                    {"contentDetails": {"videoId": v}} for v in PLAYLIST
                ]
            })
        elif route.path == "/youtube/v3/channels":
            self._json({
                "items": [{
                    "id": "UCstub0000000000000000",
                    "contentDetails": {"relatedPlaylists": {"uploads": "UUstub"}},
                }]
            })
        else:
            self._send(404, "not found", "text/plain")

    def do_POST(self):  # noqa: N802
        route = urlparse(self.path)
        if route.path != "/youtubei/v1/player":
            self._send(404, "not found", "text/plain")
            return

        length = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(length) or b"{}")
        client = (
            ((body.get("context") or {}).get("client") or {}).get("clientName", "")
        )
        video_id = body.get("videoId", "")

        # The behaviour the whole rotation exists for: this identity is refused
        # from here, and the next one has to be tried.
        if client == "ANDROID":
            self._json({
                "playabilityStatus": {
                    "status": "LOGIN_REQUIRED",
                    "reason": "Sign in to confirm you're not a bot",
                }
            })
            return

        if TRANSCRIPTS.get(video_id) is None:
            # Answers fine, simply has no captions. Must not be read as a
            # refusal, or a working route gets retired for it.
            self._json({
                "playabilityStatus": {"status": "OK"},
                "captions": {"playerCaptionsTracklistRenderer": {"captionTracks": []}},
            })
            return

        base = f"http://{self.headers.get('Host')}/captions?v={video_id}"
        self._json({
            "playabilityStatus": {"status": "OK"},
            "videoDetails": {"videoId": video_id, "title": f"Test video {video_id}"},
            "captions": {
                "playerCaptionsTracklistRenderer": {
                    "captionTracks": [
                        {"languageCode": "de", "baseUrl": base, "kind": "asr"},
                        {"languageCode": "de", "baseUrl": base},
                    ]
                }
            },
        })

    def _json(self, payload):
        self._send(200, json.dumps(payload), "application/json")

    def _send(self, status, body, content_type):
        raw = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8099), Handler).serve_forever()
