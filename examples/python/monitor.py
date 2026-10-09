#!/usr/bin/env python3
"""Local, read-only SQL log monitor. No database access."""

import argparse
import collections
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import stat
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

MAX_LINE = 1024 * 1024
MAX_SQL = 64 * 1024
MAX_TEMPLATE = 16 * 1024
MAX_ERROR = 8192
READ_CHUNK = 64 * 1024
ANSI = re.compile(r"\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07\x1b]*(?:\x07|\x1b\\))")
DURATION = re.compile(r"(\d+(?:\.\d*)?|\.\d+)(ns|us|µs|μs|ms|s|m|h)")
NUMBER = re.compile(r"(?:0[xX][0-9a-fA-F]+|0[bB][01]+|(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)")
COMPACT = re.compile(r"^\[([^\]\r\n]+)\][ \t]+(DEBUG|INFO|WARN|WARNING|ERROR|FATAL|TRACE)[ \t]+\[([^\]\r\n]*)\][ \t]+(?:\[([^\]\r\n]*)\][ \t]+)?([^\s\[][^\r\n]*?)[ \t]+-[ \t]+([^\r\n]*)$")
KINDS = {"SQL Query": "query", "Slow SQL": "slow", "SQL Error": "error"}


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def duration_ms(value):
    if not isinstance(value, str):
        return None
    value = value.strip()
    if value == "0":
        return 0.0
    sign = -1 if value.startswith("-") else 1
    if value.startswith(("-", "+")):
        value = value[1:]
    factors = {"ns": 0.000001, "us": 0.001, "µs": 0.001, "μs": 0.001,
               "ms": 1, "s": 1000, "m": 60000, "h": 3600000}
    pos = 0
    total = 0.0
    for match in DURATION.finditer(value):
        if match.start() != pos:
            return None
        total += float(match[1]) * factors[match[2]]
        pos = match.end()
    if pos != len(value) or not pos or not math.isfinite(total):
        return None
    return sign * total


def quoted_end(sql, start, quote):
    end_quote = "]" if quote == "[" else quote
    i = start + 1
    while i < len(sql):
        if sql[i] == "\\":
            i += 2
        elif sql[i] == end_quote:
            if i + 1 < len(sql) and sql[i + 1] == end_quote:
                i += 2
            else:
                return i + 1, True
        else:
            i += 1
    return len(sql), False


def sql_tokens(sql):
    """Yield (kind, text, closed); literals and comments never reach templates."""
    i = 0
    while i < len(sql):
        start = i
        c = sql[i]
        if c.isspace():
            i += 1
            while i < len(sql) and sql[i].isspace():
                i += 1
            yield "space", " ", True
        elif sql.startswith("--", i) or c == "#":
            end = sql.find("\n", i)
            i = len(sql) if end < 0 else end
            yield "comment", sql[start:i], True
        elif sql.startswith("/*", i):
            i += 2
            depth = 1
            while i < len(sql) and depth:
                if sql.startswith("/*", i):
                    depth += 1
                    i += 2
                elif sql.startswith("*/", i):
                    depth -= 1
                    i += 2
                else:
                    i += 1
            yield "comment", sql[start:i], depth == 0
        elif c in ("'", '"') or (c in "eEnNxXbB" and i + 1 < len(sql) and sql[i + 1] in ("'", '"')):
            # MySQL strings use either quote; only backticks/brackets are identifiers.
            quote_start = i if c in ("'", '"') else i + 1
            i, closed = quoted_end(sql, quote_start, sql[quote_start])
            yield "literal", sql[start:i], closed
        elif c in ('`', '['):
            i, closed = quoted_end(sql, i, c)
            yield "identifier", sql[start:i], closed
        elif c == "$" and re.match(r"\$(?:[A-Za-z_][A-Za-z_0-9]*)?\$", sql[i:]):
            delimiter = re.match(r"\$(?:[A-Za-z_][A-Za-z_0-9]*)?\$", sql[i:])[0]
            end = sql.find(delimiter, i + len(delimiter))
            i = len(sql) if end < 0 else end + len(delimiter)
            yield "literal", sql[start:i], end >= 0
        elif c.isalpha() or c in "_$":
            i += 1
            while i < len(sql) and (sql[i].isalnum() or sql[i] in "_$"):
                i += 1
            yield "identifier", sql[start:i], True
        elif c.isdigit() or (c == "." and i + 1 < len(sql) and sql[i + 1].isdigit()):
            match = NUMBER.match(sql, i)
            if match is None:
                i += 1
                yield "literal", c, True
                continue
            i = match.end()
            # MySQL permits unquoted identifiers beginning with digits.
            if i < len(sql) and (sql[i].isalpha() or sql[i] in "_$"):
                while i < len(sql) and (sql[i].isalnum() or sql[i] in "_$"):
                    i += 1
                yield "identifier", sql[start:i], True
            else:
                yield "literal", sql[start:i], True
        else:
            i += 1
            yield "symbol", c, True


def redact_sql(sql):
    pieces = []
    truncated = sql.rstrip().endswith("...")
    depth = 0
    last = ""
    for kind, text, closed in sql_tokens(sql):
        truncated |= not closed
        if kind == "symbol" and text == "(":
            depth += 1
        elif kind == "symbol" and text == ")":
            depth -= 1
            truncated |= depth < 0
        if kind not in ("space", "comment") and text != ";":
            last = text.upper() if kind == "identifier" else "?" if kind == "literal" else text
        if kind == "literal" or not closed:
            pieces.append("?")
        elif kind == "comment":
            pieces.append("/* redacted */")
        else:
            pieces.append(text)
    truncated |= depth != 0 or last in {"SELECT", "FROM", "WHERE", "JOIN", "ON", "AND", "OR", "BY", "HAVING", "INTO", "VALUES", "SET", "AS", "UNION", "WHEN", "THEN", "ELSE", "BETWEEN", "IN", "IS", "NOT", "LIKE", "LIMIT", "OFFSET", "RETURNING", "=", ",", "+", "-", "/", "<", ">", "|", "&"}
    template = "".join(pieces).strip()
    fingerprint = hashlib.sha256(template.encode("utf-8", errors="replace")).hexdigest()[:16]
    if len(template) > MAX_TEMPLATE:
        template = template[:MAX_TEMPLATE] + "…"
        truncated = True
    return template, fingerprint, bool(truncated)


def clean_text(value, limit=512):
    if not isinstance(value, (str, int, float)) or isinstance(value, bool):
        return ""
    return "".join(c for c in ANSI.sub("", str(value)) if c >= " " and c != "\x7f")[:limit]


def trace_value(value, size):
    if isinstance(value, str) and re.fullmatch(r"[0-9a-fA-F]{%d}" % size, value) and int(value, 16):
        return value.lower()
    return ""


def rows_value(value):
    if isinstance(value, int) and not isinstance(value, bool):
        return value if abs(value) <= 2**53 else None
    if isinstance(value, str) and re.fullmatch(r"-?\d{1,16}", value.strip()):
        return rows_value(int(value))
    return None


def compact_record(line):
    match = COMPACT.fullmatch(line)
    if not match:
        if re.search(r"(?: - | \| )(?:SQL Query|Slow SQL|SQL Error)(?: \||$)", line):
            raise ValueError("Malformed compact SQL record")
        return None
    timestamp, level, service, metadata, caller, body = match.groups()
    parts = body.split(" | ")
    source = caller
    if parts and parts[0].startswith("source="):
        source = parts.pop(0)[7:]
    if not parts or parts[0] not in KINDS:
        return None
    message = parts.pop(0)
    record = {"ts": timestamp, "level": level, "source": source, "msg": message}
    for field in (metadata or "").split():
        name, separator, value = field.partition(":")
        if separator and name in ("trace", "span"):
            record[name + ".id"] = trace_value(value, 32 if name == "trace" else 16)
    # CompactLogger leaves SQL newlines unescaped. A physical line can be a
    # complete-looking prefix; without framing, completeness cannot be proven.
    record["truncated"] = True
    while parts:
        part = parts[0]
        if part.startswith("sql="):
            parts[0] = part[4:]
            break
        if part.startswith("elapsed="):
            record["elapsed"] = part[8:]
        elif part.startswith("rows="):
            record["rows"] = part[5:]
        elif duration_ms(part) is not None:
            record["elapsed"] = part
        elif re.fullmatch(r"-?\d+ rows", part):
            record["rows"] = part[:-5]
        elif part.startswith("threshold:"):
            pass
        else:
            break
        parts.pop(0)
    sql = " | ".join(parts)
    # An error suffix is metadata only outside SQL literals/identifiers/comments.
    offset = 0
    for kind, text, closed in sql_tokens(sql):
        if kind == "space" and sql.startswith(" | error:", offset):
            record["error"] = sql[offset + len(" | error:"):].strip()
            sql = sql[:offset]
            break
        # Token whitespace is normalized; use the original span for offsets.
        if kind == "space":
            while offset < len(sql) and sql[offset].isspace():
                offset += 1
        else:
            offset += len(text)
    if not sql.strip():
        raise ValueError("Missing SQL")
    first = next((text.upper() for kind, text, _ in sql_tokens(sql)
                  if kind not in ("space", "comment") and text != "("), "")
    if first not in {"SELECT", "INSERT", "UPDATE", "DELETE", "REPLACE", "MERGE", "WITH", "CREATE", "ALTER", "DROP", "TRUNCATE", "SHOW", "EXPLAIN", "DESCRIBE", "DESC", "BEGIN", "START", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "SET", "USE", "PRAGMA", "CALL", "EXEC", "EXECUTE", "PREPARE", "DEALLOCATE", "GRANT", "REVOKE", "ANALYZE", "VACUUM", "LOCK", "UNLOCK", "DO", "VALUES", "TABLE", "COMMENT", "RENAME", "FLUSH", "KILL", "LOAD", "COPY", "REINDEX", "ATTACH", "DETACH"}:
        raise ValueError("Malformed compact SQL")
    record["sql"] = sql
    return record


def parse_line(line):
    if isinstance(line, bytes):
        try:
            line = line.decode("utf-8")
        except UnicodeDecodeError:
            raise ValueError("Invalid UTF-8") from None
    line = ANSI.sub("", line).rstrip("\r\n")
    if not line.strip():
        return None
    if line.lstrip().startswith("{"):
        try:
            record = json.loads(line)
        except (ValueError, RecursionError):
            raise ValueError("Malformed JSON") from None
        if not isinstance(record, dict) or not isinstance(record.get("msg"), str) or record["msg"] not in KINDS:
            return None
    else:
        record = compact_record(line)
        if record is None:
            return None
    sql = record.get("sql")
    if not isinstance(sql, str) or not sql.strip():
        raise ValueError("Missing SQL")
    raw_error = record.get("error", "")
    if not isinstance(raw_error, str):
        raw_error = ""
    raw = sql[:MAX_SQL]
    template, fingerprint, truncated = redact_sql(raw)
    truncated |= len(sql) > MAX_SQL or len(raw_error) > MAX_ERROR or record.get("truncated") is True
    kind = KINDS[record["msg"]]
    event = {"time": clean_text(record.get("ts", "")), "received_at": now(),
             "level": clean_text(record.get("level", "")).upper(), "kind": kind,
             "sql": template, "fingerprint": fingerprint,
             "elapsed_ms": duration_ms(record.get("elapsed")), "rows": rows_value(record.get("rows")),
             "source": clean_text(record.get("source", "")),
             "trace_id": trace_value(record.get("trace.id"), 32),
             "span_id": trace_value(record.get("span.id"), 16),
             "error": "SQL execution error (details available on explicit request)" if raw_error or kind == "error" else "",
             "truncated": bool(truncated)}
    return event, {"sql": raw, "error": raw_error[:MAX_ERROR]}


class Monitor:
    def __init__(self, path, max_events=2000, initial_bytes=2 * 1024 * 1024):
        if not 1 <= max_events <= 2000 or initial_bytes < 0:
            raise ValueError("capacity must be 1..2000; initial_bytes must be nonnegative")
        self.path = os.path.abspath(os.fspath(path))
        self.initial_bytes = initial_bytes
        self.lock = threading.RLock()
        self.events = collections.deque()
        self.raw = {}
        self.next_id = 1
        self.status = {"path": self.path, "exists": False, "bytes_read": 0, "lines_seen": 0,
                       "ignored_lines": 0, "parse_errors": 0, "rotations": 0,
                       "dropped_events": 0, "oversize_lines": 0, "started_at": now(),
                       "last_read": None, "capacity": max_events, "error": ""}
        self.file = None
        self.identity = None
        self.offset = 0
        self.boundary = b""
        self.pending = bytearray()
        self.discard = False
        self.initialized = False
        self.stop_event = threading.Event()
        self.thread = None

    def snapshot(self):
        with self.lock:
            return {"status": dict(self.status), "events": [dict(event) for event in self.events]}

    def raw_event(self, event_id):
        with self.lock:
            raw = self.raw.get(event_id)
            return dict(raw) if raw is not None else None

    def consume(self, chunk):
        for index, part in enumerate(chunk.split(b"\n")):
            if index:
                self.status["lines_seen"] += 1
                if not self.discard:
                    try:
                        result = parse_line(bytes(self.pending))
                    except ValueError:
                        self.status["parse_errors"] += 1
                    else:
                        if result is None:
                            self.status["ignored_lines"] += 1
                        else:
                            event, raw = result
                            event["id"] = self.next_id
                            self.next_id += 1
                            if len(self.events) == self.status["capacity"]:
                                self.raw.pop(self.events.popleft()["id"], None)
                                self.status["dropped_events"] += 1
                            self.events.append(event)
                            self.raw[event["id"]] = raw
                self.pending.clear()
                self.discard = False
            if not self.discard:
                if len(self.pending) + len(part) > MAX_LINE:
                    self.pending.clear()
                    self.discard = True
                    self.status["oversize_lines"] += 1
                else:
                    self.pending.extend(part)

    def _open(self, info):
        descriptor = os.open(self.path, os.O_RDONLY | os.O_NONBLOCK)
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            os.close(descriptor)
            raise OSError("Not a regular file")
        new_file = os.fdopen(descriptor, "rb", buffering=0)
        if self.file is not None:
            self.file.close()
        self.file = new_file
        self.identity = (opened.st_dev, opened.st_ino)
        self.pending.clear()
        self.discard = False
        self.offset = max(0, opened.st_size - self.initial_bytes) if not self.initialized else 0
        if self.initialized:
            self.status["rotations"] += 1
        self.initialized = True
        if self.offset:
            previous = os.pread(descriptor, 1, self.offset - 1)
            self.discard = previous != b"\n"
        self.file.seek(self.offset)
        self.boundary = os.pread(descriptor, min(256, self.offset), max(0, self.offset - 256))

    def poll(self):
        with self.lock:
            try:
                info = os.stat(self.path)
                self.status["exists"] = True
                if not stat.S_ISREG(info.st_mode):
                    raise OSError("Not a regular file")
                if self.file is None or (info.st_dev, info.st_ino) != self.identity:
                    self._open(info)
                else:
                    boundary = os.pread(self.file.fileno(), len(self.boundary), self.offset - len(self.boundary))
                    if info.st_size < self.offset or boundary != self.boundary:
                        self.file.seek(0)
                        self.offset = 0
                        self.boundary = b""
                        self.pending.clear()
                        self.discard = False
                        self.status["rotations"] += 1
                # Bound each poll so snapshots and shutdown cannot starve on a busy log.
                for _ in range(32):
                    chunk = self.file.read(READ_CHUNK)
                    if not chunk:
                        break
                    self.offset += len(chunk)
                    self.boundary = (self.boundary + chunk)[-256:]
                    self.status["bytes_read"] += len(chunk)
                    self.status["last_read"] = now()
                    self.consume(chunk)
                self.status["error"] = ""
            except FileNotFoundError:
                self.status["exists"] = False
                self.status["error"] = "Log file is missing; retrying"
            except OSError:
                self.status["error"] = "Cannot read log file; retrying"

    def start(self):
        if self.thread is not None:
            return
        def run():
            while not self.stop_event.is_set():
                self.poll()
                self.stop_event.wait(0.2)
        self.thread = threading.Thread(target=run, name="sql-log-tail", daemon=True)
        self.thread.start()

    def close(self):
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=3)
        with self.lock:
            if self.file is not None:
                self.file.close()
                self.file = None


class MonitorServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, monitor, port=8767):
        self.monitor = monitor
        self.index_path = Path(__file__).resolve().with_name("index.html")
        super().__init__(("127.0.0.1", port), MonitorHandler)

    def get_request(self):
        connection, address = super().get_request()
        connection.settimeout(5)
        return connection, address


class MonitorHandler(BaseHTTPRequestHandler):
    server_version = "SQLMonitor"
    sys_version = ""

    def log_message(self, format, *args):
        pass

    def send_data(self, code, data, content_type="application/json; charset=utf-8"):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Content-Security-Policy", "default-src 'none'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; font-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        if self.command != "HEAD":
            try:
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError, TimeoutError):
                pass

    def send_json(self, code, value):
        self.send_data(code, json.dumps(value, ensure_ascii=True, allow_nan=False).encode("utf-8"))

    def trusted(self):
        port = self.server.server_port
        allowed = {"127.0.0.1:%d" % port, "localhost:%d" % port}
        if port == 80:
            allowed.update({"127.0.0.1", "localhost"})
        hosts = self.headers.get_all("Host", [])
        origins = self.headers.get_all("Origin", [])
        if len(hosts) != 1 or hosts[0].lower() not in allowed:
            return False
        if origins and (len(origins) != 1 or origins[0].lower() != "http://" + hosts[0].lower()):
            return False
        return self.headers.get("Sec-Fetch-Site", "") != "cross-site"

    def do_GET(self):
        if not self.trusted():
            self.send_json(403, {"error": "Untrusted host or origin"})
            return
        if self.path == "/":
            try:
                data = self.server.index_path.read_bytes()
            except OSError:
                self.send_json(404, {"error": "UI not installed"})
            else:
                self.send_data(200, data, "text/html; charset=utf-8")
        elif self.path == "/api/state":
            self.send_json(200, self.server.monitor.snapshot())
        elif re.fullmatch(r"/api/event/[0-9]{1,20}\?raw=1", self.path):
            event_id = int(urlsplit(self.path).path.rsplit("/", 1)[1])
            raw = self.server.monitor.raw_event(event_id)
            self.send_json(200 if raw is not None else 404, raw if raw is not None else {"error": "Event not found"})
        else:
            self.send_json(404, {"error": "Not found"})

    def read_only(self):
        if not self.trusted():
            self.send_json(403, {"error": "Untrusted host or origin"})
        else:
            self.send_json(405, {"error": "Read-only server; use GET"})

    do_POST = do_PUT = do_PATCH = do_DELETE = do_OPTIONS = do_HEAD = do_TRACE = do_CONNECT = read_only

    def send_error(self, code, message=None, explain=None):
        self.send_json(code, {"error": "HTTP request rejected"})


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", default="/tmp/data-service-local.log")
    parser.add_argument("--port", type=int, default=8767)
    parser.add_argument("--max-events", type=int, default=2000)
    parser.add_argument("--initial-bytes", type=int, default=2 * 1024 * 1024)
    args = parser.parse_args(argv)
    if not 1 <= args.port <= 65535 or not 1 <= args.max_events <= 2000 or args.initial_bytes < 0:
        parser.error("port must be 1..65535, max-events 1..2000, initial-bytes nonnegative")
    monitor = Monitor(args.log, args.max_events, args.initial_bytes)
    try:
        server = MonitorServer(monitor, args.port)
    except OSError:
        parser.exit(1, "Cannot bind local HTTP server\n")
    stopped = threading.Event()
    def stop(signum, frame):
        stopped.set()
    previous = {sig: signal.signal(sig, stop) for sig in (signal.SIGINT, signal.SIGTERM)}
    worker = threading.Thread(target=server.serve_forever, name="sql-monitor-http", daemon=True)
    monitor.start()
    worker.start()
    try:
        stopped.wait()
    finally:
        server.shutdown()
        server.server_close()
        monitor.close()
        worker.join(timeout=3)
        for sig, handler in previous.items():
            signal.signal(sig, handler)


if __name__ == "__main__":
    main()
