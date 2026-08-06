#!/usr/bin/env python3
"""
Supabase Advanced — ingest layer, YouTube transcripts.

A port of the workflow that has been fetching these in production
(`docs/n8n/Youtube Transcript Generator MCP.json`), with the parts that were
duplicated per entry point collapsed and the parts that were constants moved
into the database.

What it actually does, which is more than "download the subtitles":

  1. Metadata comes from the official Data API with an API key. Ordinary
     request, no proxy, no impersonation.
  2. The public watch page is loaded to lift the `INNERTUBE_API_KEY` out of the
     HTML. InnerTube is the interface YouTube's own apps talk to.
  3. The player is asked for the caption tracks *as a YouTube app* — as
     ANDROID, then WEB, then MWEB, then IOS, then the TV client. This call goes
     through the proxy. When one identity is refused, the next is tried.
  4. The caption track is chosen by language, and downloaded, again through the
     proxy.

Step 3 is the part that matters. It is not a single fallback step but a chain
of its own, and it is the most effective defence in the whole procedure. The
identities live in `transcript_provider.clients` because their version numbers
go stale, and updating them should not require a release.

HTTP goes through `curl` rather than urllib for one blunt reason: the standard
library cannot speak SOCKS5, and the proxy is a SOCKS5 proxy. Shelling out to a
tool is the same trade already made for `psql` and `pdftotext` — one apt
package instead of a dependency tree, and a failure that can be reproduced by
hand on the command line.
"""

from __future__ import annotations

import html
import json
import logging
import os
import re
import subprocess
import xml.etree.ElementTree as ElementTree
from urllib.parse import urlencode

LOG = logging.getLogger("ingest.fetcher")

# Overridable so the end-to-end test can point the whole chain at a stand-in
# and exercise the real requests — client rotation, refusals, caption download
# — without depending on YouTube being reachable or in a particular mood.
# They also give somewhere to point if YouTube moves an endpoint.
_YT = os.environ.get("INGEST_YT_BASE", "https://www.youtube.com").rstrip("/")
_YT_API = os.environ.get(
    "INGEST_YT_API_BASE", "https://www.googleapis.com/youtube/v3"
).rstrip("/")

WATCH_URL = _YT + "/watch?v={video_id}"
PLAYER_URL = _YT + "/youtubei/v1/player"
DATA_API = _YT_API

# Lifted from the page source, the same way the workflow does it.
_API_KEY_RE = re.compile(r'"INNERTUBE_API_KEY"\s*:\s*"([^"]+)"')

# YouTube says no in several different ways. Treating an UNPLAYABLE response
# that also carries no captions as a refusal matters: it is what a blocked
# client looks like when it is being polite about it.
_REFUSALS = ("Sign in to confirm", "no longer supported", "bot")


class TranscriptError(RuntimeError):
    """No transcript could be obtained by this route."""


class BlockedError(TranscriptError):
    """This client identity was refused — the next one may still work."""


class NoCaptionsError(TranscriptError):
    """The video genuinely has no captions. Trying another route will not help."""


# --------------------------------------------------------------------------
# HTTP through curl
# --------------------------------------------------------------------------


class Http:
    """Minimal HTTP client that can go through a SOCKS5 proxy."""

    def __init__(self, proxy: str | None = None, timeout: int = 20) -> None:
        self.proxy = proxy or ""
        self.timeout = timeout
        # A transcript is text; anything far larger is not a transcript, and
        # capping it keeps a hostile or broken response from being read into
        # memory in full.
        self.max_bytes = int(os.environ.get("INGEST_MAX_FETCH_MB", "16")) * 1024 * 1024

    def get(self, url: str, headers: dict | None = None, use_proxy: bool = True) -> str:
        """A GET whose URL may contain a secret.

        Every option, including the URL, is handed to curl on stdin through
        `--config -`, so nothing sensitive appears in the process list. A Data
        API key sits in the query string, and `ps` is readable by every account
        on the machine.
        """
        config = self._config_lines(url, headers or {}, use_proxy)
        return self._run(["curl", "--config", "-"], config)

    def post_json(
        self, url: str, body: dict, headers: dict | None = None, use_proxy: bool = True
    ) -> dict:
        """A POST with a JSON body.

        The body goes on stdin, so the config-file trick above is not available
        and the URL is an argument. That is acceptable here and only here: the
        one POST this module makes carries the InnerTube key, which is not a
        secret — it is published in the HTML of every watch page.
        """
        cmd = self._base_flags(use_proxy) + ["--data-binary", "@-"]
        merged = {"Content-Type": "application/json", **(headers or {})}
        for name, value in merged.items():
            cmd += ["-H", f"{name}: {value}"]
        cmd.append(url)
        raw = self._run(cmd, json.dumps(body))
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise TranscriptError(f"expected JSON from {url}: {raw[:200]!r}") from exc

    def _base_flags(self, use_proxy: bool) -> list[str]:
        cmd = [
            "curl", "-sS", "--fail-with-body",
            "--compressed",
            "--location", "--max-redirs", "3",
            "--max-time", str(self.timeout),
            "--max-filesize", str(self.max_bytes),
        ]
        if use_proxy and self.proxy:
            # socks5h, not socks5: the "h" makes the proxy resolve the name.
            # Resolving locally would leak which hosts are being visited and
            # partly defeat the point of routing through it at all.
            cmd += ["--proxy", self.proxy]
        return cmd

    def _config_lines(self, url: str, headers: dict, use_proxy: bool) -> str:
        """curl's config-file format. Values are quoted, quotes are escaped."""
        def quote(value: str) -> str:
            return '"' + str(value).replace("\\", "\\\\").replace('"', '\\"') + '"'

        lines = [
            "silent", "show-error", "fail-with-body", "compressed", "location",
            f"max-redirs = 3",
            f"max-time = {self.timeout}",
            f"max-filesize = {self.max_bytes}",
            f"url = {quote(url)}",
        ]
        if use_proxy and self.proxy:
            lines.append(f"proxy = {quote(self.proxy)}")
        for name, value in headers.items():
            lines.append(f"header = {quote(f'{name}: {value}')}")
        return "\n".join(lines) + "\n"

    def _run(self, cmd: list[str], stdin: str | None) -> str:
        try:
            proc = subprocess.run(
                cmd,
                input=stdin,
                capture_output=True,
                text=True,
                timeout=self.timeout + 10,
            )
        except FileNotFoundError as exc:
            raise TranscriptError("curl is not installed") from exc
        except subprocess.TimeoutExpired as exc:
            raise TranscriptError(f"request timed out after {self.timeout}s") from exc

        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "").strip()[:300]
            # curl's exit codes for "cannot reach the proxy" are worth naming
            # separately: the fix is somewhere else than a YouTube problem.
            if proc.returncode in (5, 7, 97):
                raise TranscriptError(
                    f"cannot reach the proxy {self.proxy!r}: {detail}"
                )
            raise TranscriptError(f"request failed (curl {proc.returncode}): {detail}")
        return proc.stdout


# --------------------------------------------------------------------------
# The transcript itself
# --------------------------------------------------------------------------


def innertube_key(http: Http, video_id: str) -> str:
    """Reads the InnerTube key out of the public watch page.

    Not proxied: this is the same page any visitor loads, and spending proxy
    requests on it would burn the scarce resource on the cheap step.
    """
    page = http.get(
        WATCH_URL.format(video_id=video_id),
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
        use_proxy=False,
    )
    found = _API_KEY_RE.search(page)
    if not found:
        raise TranscriptError(
            "no InnerTube key in the watch page — the video is probably "
            "private, age-restricted or removed"
        )
    return found.group(1)


def player_response(
    http: Http, video_id: str, api_key: str, client: dict, use_proxy: bool
) -> dict:
    """Asks the player for this video, impersonating one client."""
    context = {
        "client": {
            "clientName": client["clientName"],
            "clientVersion": client["clientVersion"],
            "hl": client.get("hl", "en"),
            "timeZone": "UTC",
            "utcOffsetMinutes": 0,
        }
    }
    if "androidSdkVersion" in client:
        context["client"]["androidSdkVersion"] = client["androidSdkVersion"]

    return http.post_json(
        f"{PLAYER_URL}?key={api_key}",
        {"context": context, "videoId": video_id},
        headers={"User-Agent": client.get("userAgent", "")},
        use_proxy=use_proxy,
    )


def caption_tracks(response: dict) -> list[dict]:
    renderer = (response.get("captions") or {}).get(
        "playerCaptionsTracklistRenderer"
    ) or {}
    return renderer.get("captionTracks") or []


def is_refusal(response: dict, tracks: list[dict]) -> bool:
    """Whether this response means "not from you", rather than "not at all".

    An UNPLAYABLE status with no captions counts: that is what a refused
    client identity looks like when the answer is phrased as unavailability.
    """
    status = (response.get("playabilityStatus") or {}).get("status", "")
    reason = (response.get("playabilityStatus") or {}).get("reason", "") or ""
    if status in ("LOGIN_REQUIRED", "ERROR"):
        return True
    if status == "UNPLAYABLE" and not tracks:
        return True
    return any(marker.lower() in reason.lower() for marker in _REFUSALS)


def choose_track(tracks: list[dict], preferred: str | None) -> dict:
    """Picks the caption track to download.

    Preference order: the asked-for language, then any manually written track,
    then whatever exists. Manual captions before automatic ones because
    automatic ones carry no punctuation, and sentence-mode chunking has nothing
    to cut on without it.
    """
    if preferred:
        for track in tracks:
            if track.get("languageCode") == preferred and track.get("kind") != "asr":
                return track
        for track in tracks:
            if track.get("languageCode") == preferred:
                return track
    for track in tracks:
        if track.get("kind") != "asr":
            return track
    return tracks[0]


def parse_transcript(payload: str) -> tuple[str, list[dict]]:
    """Turns the caption file into plain text and timed segments.

    Returns the text a human would read, plus the segments, so a passage can
    later be pointed back into the video.
    """
    try:
        root = ElementTree.fromstring(payload)
    except ElementTree.ParseError as exc:
        raise TranscriptError(f"caption track is not parseable XML: {exc}") from exc

    segments = []
    for node in root.iter("text"):
        content = html.unescape((node.text or "").replace("\n", " ")).strip()
        if not content:
            continue
        start = float(node.get("start") or 0.0)
        duration = float(node.get("dur") or 0.0)
        segments.append({
            "start_ms": int(start * 1000),
            "end_ms": int((start + duration) * 1000),
            "text": content,
        })
    if not segments:
        raise NoCaptionsError("the caption track downloaded but contained no text")

    return " ".join(s["text"] for s in segments), segments


def fetch_via(
    provider: dict, video_id: str, preferred_language: str | None, proxy: str | None
) -> dict:
    """Fetches one transcript through one provider, rotating its clients.

    Raises BlockedError only when *every* client of this provider was refused,
    which is the signal that the route rather than the video is the problem.
    """
    use_proxy = bool(provider.get("use_proxy"))
    http = Http(proxy=proxy if use_proxy else None)
    api_key = innertube_key(http, video_id)

    refusals = []
    for client in provider.get("clients") or []:
        name = client.get("clientName", "?")
        try:
            response = player_response(http, video_id, api_key, client, use_proxy)
        except TranscriptError as exc:
            refusals.append(f"{name}: {exc}")
            continue

        tracks = caption_tracks(response)
        if is_refusal(response, tracks):
            status = (response.get("playabilityStatus") or {}).get("status", "?")
            refusals.append(f"{name}: refused ({status})")
            LOG.info("client %s refused for %s, trying the next", name, video_id)
            continue

        if not tracks:
            # The video answered, and has no captions. Another identity will
            # not conjure them, and neither will another provider.
            raise NoCaptionsError(f"{video_id} has no caption tracks")

        track = choose_track(tracks, preferred_language)
        payload = http.get(track["baseUrl"], use_proxy=use_proxy)
        text, segments = parse_transcript(payload)
        return {
            "text": text,
            "segments": segments,
            "language": track.get("languageCode"),
            "automatic": track.get("kind") == "asr",
            "client": name,
            "available_languages": sorted(
                {t.get("languageCode") for t in tracks if t.get("languageCode")}
            ),
        }

    raise BlockedError(
        f"every client of {provider['key']} was refused for {video_id}: "
        + "; ".join(refusals[:5])
    )


# --------------------------------------------------------------------------
# Metadata and listings — the official API
# --------------------------------------------------------------------------


def _api(http: Http, path: str, params: dict) -> dict:
    """One Data API call. Never proxied — this is an authenticated,
    rate-limited API that has no objection to where the request comes from,
    and proxy requests are the scarce resource."""
    query = urlencode({k: v for k, v in params.items() if v is not None})
    raw = http.get(f"{DATA_API}/{path}?{query}", use_proxy=False)
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise TranscriptError(f"Data API returned no JSON: {raw[:200]!r}") from exc
    if "error" in parsed:
        message = (parsed["error"] or {}).get("message", "unknown error")
        raise TranscriptError(f"Data API refused: {message}")
    return parsed


# --------------------------------------------------------------------------
# Working out what someone pasted
# --------------------------------------------------------------------------

_TARGET_PATTERNS = [
    ("playlist", re.compile(r"[?&]list=([A-Za-z0-9_-]{12,})")),
    ("video", re.compile(r"[?&]v=([A-Za-z0-9_-]{11})(?:[^A-Za-z0-9_-]|$)")),
    ("video", re.compile(r"youtu\.be/([A-Za-z0-9_-]{11})")),
    ("video", re.compile(r"youtube\.com/(?:shorts|embed|live)/([A-Za-z0-9_-]{11})")),
    ("channel", re.compile(r"youtube\.com/channel/(UC[A-Za-z0-9_-]{20,})")),
    ("handle", re.compile(r"youtube\.com/@([A-Za-z0-9_.-]+)")),
]


def parse_target(text: str) -> tuple[str, str]:
    """Works out whether a pasted string is a video, a playlist or a channel.

    Accepts full URLs in their several shapes as well as bare ids, because
    what people have to hand is a link, not an id — and being made to work out
    which kind of link it is is exactly the friction this is meant to remove.

    A playlist link wins over the video in it: "youtube.com/watch?v=X&list=Y"
    is what you get from clicking a video inside a playlist, and someone who
    pastes it means the playlist.
    """
    value = (text or "").strip()
    if not value:
        raise TranscriptError("nothing to parse")

    for kind, pattern in _TARGET_PATTERNS:
        found = pattern.search(value)
        if found:
            return kind, found.group(1)

    # Bare ids, distinguished by the prefixes YouTube assigns.
    if re.fullmatch(r"UC[A-Za-z0-9_-]{20,}", value):
        return "channel", value
    if re.fullmatch(r"(PL|UU|LL|FL|OL|RD)[A-Za-z0-9_-]{10,}", value):
        return "playlist", value
    if re.fullmatch(r"[A-Za-z0-9_-]{11}", value):
        return "video", value
    if value.startswith("@"):
        return "handle", value[1:]

    raise TranscriptError(
        f"cannot tell what {text!r} refers to — paste a video, playlist or "
        "channel link, or an id"
    )


def resolve_handle(api_key: str, handle: str) -> str:
    """Turns an @handle into the channel id the rest of the code works with."""
    http = Http()
    data = _api(http, "channels", {
        "part": "id",
        "forHandle": f"@{handle.lstrip('@')}",
        "key": api_key,
    })
    items = data.get("items") or []
    if not items:
        raise TranscriptError(f"no channel with handle @{handle}")
    return items[0]["id"]


def video_metadata(api_key: str, video_id: str) -> dict:
    """Title, channel, publication date and duration for one video."""
    http = Http()
    data = _api(http, "videos", {
        "part": "snippet,contentDetails,statistics",
        "id": video_id,
        "key": api_key,
    })
    items = data.get("items") or []
    if not items:
        raise TranscriptError(f"the Data API knows no video {video_id}")
    item = items[0]
    snippet = item.get("snippet") or {}
    return {
        "video_id": video_id,
        "title": snippet.get("title"),
        "description": snippet.get("description"),
        "channel_id": snippet.get("channelId"),
        "channel_title": snippet.get("channelTitle"),
        "published_at": snippet.get("publishedAt"),
        "duration_sec": parse_duration(
            (item.get("contentDetails") or {}).get("duration", "")
        ),
    }


_DURATION_RE = re.compile(
    r"P(?:(\d+)D)?T?(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?"
)


def parse_duration(iso: str) -> int:
    """ISO 8601 duration to seconds. YouTube returns e.g. PT1H2M3S."""
    found = _DURATION_RE.match(iso or "")
    if not found:
        return 0
    days, hours, minutes, seconds = (int(g or 0) for g in found.groups())
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


def list_playlist(api_key: str, playlist_id: str, limit: int = 500) -> list[str]:
    """Every video id in a playlist, following pages until the limit."""
    http = Http()
    ids: list[str] = []
    page = None
    while len(ids) < limit:
        data = _api(http, "playlistItems", {
            "part": "contentDetails",
            "playlistId": playlist_id,
            "maxResults": 50,
            "pageToken": page,
            "key": api_key,
        })
        for item in data.get("items") or []:
            video_id = (item.get("contentDetails") or {}).get("videoId")
            if video_id:
                ids.append(video_id)
        page = data.get("nextPageToken")
        if not page:
            break
    return ids[:limit]


def list_channel(api_key: str, channel_id: str, limit: int = 500) -> list[str]:
    """Every video of a channel.

    Goes through the channel's uploads playlist rather than through search:
    search costs a hundred times more quota per call and silently omits older
    videos.
    """
    http = Http()
    data = _api(http, "channels", {
        "part": "contentDetails",
        "id": channel_id,
        "key": api_key,
    })
    items = data.get("items") or []
    if not items:
        raise TranscriptError(f"the Data API knows no channel {channel_id}")
    uploads = (
        ((items[0].get("contentDetails") or {}).get("relatedPlaylists") or {})
        .get("uploads")
    )
    if not uploads:
        raise TranscriptError(f"channel {channel_id} exposes no uploads playlist")
    return list_playlist(api_key, uploads, limit)
