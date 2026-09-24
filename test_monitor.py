import http.client
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

import monitor


def record(sql="SELECT item7 FROM sample WHERE id=42 AND name='private-value'", **fields):
    value = {"level": "DEBUG", "ts": "2026-09-23T10:00:00Z", "source": "internal/data/tag.go:549",
             "elapsed": "38.01ms", "rows": 1, "sql": sql, "msg": "SQL Query"}
    value.update(fields)
    return json.dumps(value).encode("utf-8") + b"\n"


class ParserTests(unittest.TestCase):
    def test_current_json_contract_and_raw_isolation(self):
        event, raw = monitor.parse_line(record())
        self.assertEqual(event["sql"], "SELECT item7 FROM sample WHERE id=? AND name=?")
        self.assertEqual(event["elapsed_ms"], 38.01)
        self.assertEqual(event["rows"], 1)
        self.assertEqual(event["source"], "internal/data/tag.go:549")
        self.assertEqual(event["kind"], "query")
        self.assertEqual(event["trace_id"], "")
        self.assertEqual(event["span_id"], "")
        self.assertNotIn("private-value", json.dumps(event))
        self.assertIn("private-value", raw["sql"])
        self.assertFalse(event["truncated"])
        self.assertRegex(event["received_at"], r"T.*\+00:00$")
        self.assertRegex(event["fingerprint"], r"^[a-f0-9]{16}$")

    def test_value_independent_fingerprints(self):
        one, _ = monitor.parse_line(record("SELECT col2 FROM t WHERE a=1 AND b='first'"))
        two, _ = monitor.parse_line(record("SELECT col2 FROM t WHERE a=300 AND b='second'"))
        self.assertEqual(one["fingerprint"], two["fingerprint"])
        self.assertEqual(one["sql"], two["sql"])

    def test_quoted_identifiers_and_numeric_boundaries(self):
        sql = 'SELECT col12, x2y, `table42`.`a``b`, "a""3", [col]]2], 123abc, $1 FROM t9 WHERE n=-12.3e-4 AND h=0xDEAD AND b=0b101'
        template, _, truncated = monitor.redact_sql(sql)
        self.assertEqual(template, 'SELECT col12, x2y, `table42`.`a``b`, ?, [col]]2], 123abc, $1 FROM t9 WHERE n=-? AND h=? AND b=?')
        self.assertFalse(truncated)
        self.assertEqual(monitor.redact_sql("SELECT a-1, .5, 5., +2, 1e3")[0], "SELECT a-?, ?, ?, +?, ?")

    def test_strings_and_comments_with_sql_punctuation(self):
        sql = "SELECT 'x''y; -- private', E'a\\'b', N'c', X'DEAD', B'101', $$dollar secret$$, $tag$other;--$tag$ /* comment secret */ -- last secret\nFROM `t1` WHERE n=17 # hidden\n"
        template, _, truncated = monitor.redact_sql(sql)
        self.assertEqual(template, "SELECT ?, ?, ?, ?, ?, ?, ? /* redacted */ /* redacted */ FROM `t1` WHERE n=? /* redacted */")
        self.assertFalse(truncated)
        self.assertNotIn("secret", template)
        self.assertEqual(monitor.redact_sql("SELECT 1 /* outer /* nested */ secret */")[0], "SELECT ? /* redacted */")

    def test_double_quoted_secrets_are_raw_only(self):
        sql = r'SELECT "double-secret", "escaped\"secret", "doubled""secret", N"national-secret", "literal | error:secret" FROM `table42` WHERE [col2]="filter-secret"'
        expected = 'SELECT ?, ?, ?, ?, ? FROM `table42` WHERE [col2]=?'
        for line in (record(sql), '[t] DEBUG [svc] [] caller.go:9 - SQL Query | sql=' + sql):
            with self.subTest(line=line):
                event, raw = monitor.parse_line(line)
                self.assertEqual(event["sql"], expected)
                self.assertNotIn("secret", json.dumps(event))
                self.assertEqual(raw, {"sql": sql, "error": ""})
                other, _ = monitor.parse_line(record('SELECT "different", "values", "here", N"too", "changed" FROM `table42` WHERE [col2]="other"'))
                self.assertEqual(event["fingerprint"], other["fingerprint"])

    def test_unfinished_literals_are_redacted_and_marked(self):
        for sql in ("SELECT 'private...", "SELECT E'private\\", "SELECT $tag$private...", "SELECT /* private...", 'SELECT "private...', "SELECT `private..."):
            with self.subTest(sql=sql):
                template, _, truncated = monitor.redact_sql(sql)
                self.assertNotIn("private", template)
                self.assertTrue(truncated)
        self.assertTrue(monitor.redact_sql("SELECT a FROM t ...")[2])

    def test_incomplete_compact_sql_is_never_marked_complete(self):
        for sql in ("SELECT", "SELECT a FROM", "SELECT a FROM t WHERE id=", "SELECT a FROM t WHERE id IN (1, 2"):
            with self.subTest(sql=sql):
                event, _ = monitor.parse_line("[t] DEBUG [svc] caller - SQL Query | sql=" + sql)
                self.assertTrue(event["truncated"])
        for sql in ("SELECT '(private)'", "SELECT (1 + 2)", "SELECT `from` FROM `t9`"):
            self.assertFalse(monitor.redact_sql(sql)[2])
        for body in ("SQL Query | elapsed=bad | rows=no | request-body", "SQL Query | sql=request-body", "Slow SQL | 203ms | 5 rows | threshold:200ms"):
            with self.assertRaises(ValueError):
                monitor.parse_line("[t] WARN [svc] caller - " + body)

    def test_json_escaped_newline(self):
        event, raw = monitor.parse_line(record("SELECT 'first\nsecond'\nFROM t WHERE id=1"))
        self.assertEqual(event["sql"], "SELECT ? FROM t WHERE id=?")
        self.assertIn("first\nsecond", raw["sql"])
        self.assertFalse(event["truncated"])

    def test_go_durations(self):
        for value, expected in {"38.01ms": 38.01, "203ms": 203, "1us": .001,
                                "2µs": .002, "3μs": .003, "4ns": .000004,
                                "1m": 60000, "1m2.5s": 62500, "1h2m3s4ms5us6ns": 3723004.005006,
                                "0": 0, "-1.5ms": -1.5, "+2s": 2000,
                                ".5s": 500, "1.s": 1000}.items():
            with self.subTest(value=value):
                self.assertAlmostEqual(monitor.duration_ms(value), expected)
        for value in (None, True, 12, "", "ms", "12", "1ms garbage", "NaN", "1d", "1e3ms", "--1s", "9" * 400 + "h"):
            with self.subTest(value=value):
                self.assertIsNone(monitor.duration_ms(value))

    def test_slow_error_and_trace_ids(self):
        event, raw = monitor.parse_line(record(msg="SQL Error", error="sensitive-db-error", **{"trace.id": "A" * 32, "span.id": "B" * 16}))
        self.assertEqual(event["kind"], "error")
        self.assertEqual(event["trace_id"], "a" * 32)
        self.assertEqual(event["span_id"], "b" * 16)
        self.assertNotIn("sensitive-db-error", json.dumps(event))
        self.assertEqual(raw["error"], "sensitive-db-error")
        event, _ = monitor.parse_line(record(msg="Slow SQL", elapsed="1m2s", **{"trace.id": "0" * 32, "span.id": "0" * 16}))
        self.assertEqual((event["kind"], event["elapsed_ms"]), ("slow", 62000))
        self.assertEqual((event["trace_id"], event["span_id"]), ("", ""))

    def test_ansi_json_and_compact(self):
        event, _ = monitor.parse_line(b"\x1b[36m" + record().rstrip() + b"\x1b[0m\n")
        self.assertEqual(event["kind"], "query")
        line = "\x1b[36m[10:20:30] DEBUG [data-service] [trace:" + "1" * 32 + " span:" + "2" * 16 + "] caller.go:9 - source=internal/data/tag.go:549 | SQL Query | elapsed=38.01ms | rows=1 | sql=SELECT col7 FROM t WHERE name='a | b'\x1b[0m"
        event, raw = monitor.parse_line(line)
        self.assertEqual(event["sql"], "SELECT col7 FROM t WHERE name=?")
        self.assertEqual(event["elapsed_ms"], 38.01)
        self.assertEqual(event["rows"], 1)
        self.assertEqual(event["trace_id"], "1" * 32)
        self.assertEqual(event["source"], "internal/data/tag.go:549")
        self.assertIn("a | b", raw["sql"])

    def test_compact_actual_metadata_and_source(self):
        trace, span = "A" * 32, "B" * 16
        cases = (
            (f"[trace:{trace} span:{span} uid:private-user tenant:private-tenant] ", trace.lower(), span.lower()),
            ("[] ", "", ""),
            ("[uid:private-user tenant:private-tenant] ", "", ""),
            (f"[trace:{trace} uid:private-user tenant:private-tenant] ", trace.lower(), ""),
            (f"[tenant:private-tenant span:{span} uid:private-user trace:{trace}] ", trace.lower(), span.lower()),
            (f"[span:{span}] ", "", span.lower()),
            ("[trace:" + "0" * 32 + " span:" + "0" * 16 + "] ", "", ""),
            (f"[trace:{trace}x span:{span}x] ", "", ""),
            ("[trace:short span:not-hex unknown:value] ", "", ""),
            (f"[uid:trace:{trace} tenant:span:{span}] ", "", ""),
            ("[arbitrary metadata] ", "", ""),
            ("", "", ""),  # Legacy headers without a metadata bracket.
        )
        for metadata, expected_trace, expected_span in cases:
            for source_prefix in ("", "source=internal/data/tag.go:549 | "):
                with self.subTest(metadata=metadata, source_prefix=source_prefix):
                    line = f"[2026-09-23 10:20:30.123] DEBUG [data-service] {metadata}caller.go:9 - {source_prefix}SQL Query | elapsed=38.01ms | rows=1 | sql=SELECT col7 FROM t WHERE id=42"
                    event, raw = monitor.parse_line(line)
                    self.assertEqual(event["source"], "internal/data/tag.go:549" if source_prefix else "caller.go:9")
                    self.assertEqual((event["trace_id"], event["span_id"]), (expected_trace, expected_span))
                    self.assertEqual((event["kind"], event["elapsed_ms"], event["rows"]), ("query", 38.01, 1))
                    self.assertEqual(event["sql"], "SELECT col7 FROM t WHERE id=?")
                    self.assertEqual(raw["sql"], "SELECT col7 FROM t WHERE id=42")
                    self.assertNotIn("private-", json.dumps(event))
                    parsed = monitor.compact_record(line)
                    self.assertEqual(parsed.get("trace.id", ""), expected_trace)
                    self.assertEqual(parsed.get("span.id", ""), expected_span)

    def test_compact_slow_error_suffix(self):
        line = "[10:20:30] WARN [data-service] caller.go:9 - Slow SQL | 203ms | 5 rows | threshold:200ms | SELECT 'literal | error:inside' FROM t WHERE n=22 | error:sensitive error"
        event, raw = monitor.parse_line(line)
        self.assertEqual((event["kind"], event["elapsed_ms"], event["rows"]), ("slow", 203, 5))
        self.assertEqual(event["sql"], "SELECT ? FROM t WHERE n=?")
        self.assertEqual(raw["error"], "sensitive error")
        self.assertIn("literal | error:inside", raw["sql"])
        self.assertNotIn("sensitive", json.dumps(event))
        self.assertEqual(event["source"], "caller.go:9")
        self.assertEqual(event["trace_id"], "")

    def test_compact_truncated_and_error_kind(self):
        event, _ = monitor.parse_line("[10:20:30] ERROR [svc] caller - SQL Error | 1ms | 0 rows | SELECT 'private...")
        self.assertTrue(event["truncated"])
        self.assertEqual(event["kind"], "error")
        self.assertNotIn("private", event["sql"])
        event, _ = monitor.parse_line("[10:20:30] WARN [svc] caller - Slow SQL | 203ms | 5 rows | threshold:200ms | SELECT col FROM t ...")
        self.assertTrue(event["truncated"])

    def test_malformed_and_unrelated(self):
        for line in (b"", b"request payload SQL Query", b'{"msg":"HTTP request", "sql":"private"}',
                     b'{"msg":[]}', b'{"msg":{}}', b"FROM t", b"[t] INFO [svc] caller - unrelated"):
            self.assertIsNone(monitor.parse_line(line))
        for line in (b'{"msg":"SQL Query",', record(sql=None), record(sql=""), b"\xff", b"[broken - SQL Query | sql=SELECT 1",
                     b"[t] DEBUG [svc] caller - SQL Query | elapsed=1ms | rows=1 | sql="):
            with self.subTest(line=line):
                with self.assertRaises(ValueError):
                    monitor.parse_line(line)
        event, _ = monitor.parse_line(record(rows=True, elapsed="nonsense"))
        self.assertIsNone(event["rows"])
        self.assertIsNone(event["elapsed_ms"])
        with self.assertRaises(ValueError):
            monitor.parse_line('[t] DEBUG [svc] caller - SQL Query | sql=SELECT 1\nFROM t')

    def test_size_limits_and_unpaired_surrogate(self):
        event, raw = monitor.parse_line(record("SELECT '" + "private" * 12000 + "'", error="x" * 10000))
        self.assertEqual(event["sql"], "SELECT ?")
        self.assertTrue(event["truncated"])
        self.assertEqual(len(raw["sql"]), monitor.MAX_SQL)
        self.assertEqual(len(raw["error"]), monitor.MAX_ERROR)
        template, _, truncated = monitor.redact_sql("SELECT " + "identifier, " * 4000)
        self.assertLessEqual(len(template), monitor.MAX_TEMPLATE + 1)
        self.assertTrue(truncated)
        event, _ = monitor.parse_line(record("SELECT '\ud800'"))
        self.assertEqual(event["sql"], "SELECT ?")
        monitor.parse_line(record('SELECT "\ud800"'))


class TailTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "service.log"
        self.monitors = []

    def make_monitor(self, **kwargs):
        instance = monitor.Monitor(self.path, **kwargs)
        self.addCleanup(instance.close)
        self.monitors.append(instance)
        return instance

    def append(self, data):
        with self.path.open("ab") as file:
            file.write(data)

    def test_append_partial_and_snapshot_copy(self):
        self.path.write_bytes(record("SELECT 1"))
        instance = self.make_monitor()
        instance.poll()
        self.append(record("SELECT 2")[:-1])
        instance.poll()
        self.assertEqual(len(instance.snapshot()["events"]), 1)
        self.append(b"\n")
        instance.poll()
        snapshot = instance.snapshot()
        self.assertEqual([event["id"] for event in snapshot["events"]], [1, 2])
        self.assertEqual(snapshot["status"]["lines_seen"], 2)
        self.assertEqual(snapshot["status"]["bytes_read"], self.path.stat().st_size)
        self.assertIsNotNone(snapshot["status"]["last_read"])
        snapshot["events"][0]["sql"] = "mutated"
        snapshot["status"]["capacity"] = 7
        self.assertEqual(instance.snapshot()["events"][0]["sql"], "SELECT ?")
        self.assertEqual(instance.snapshot()["status"]["capacity"], 2000)

    def test_initial_tail_skips_only_partial_first_line(self):
        first, second = record("SELECT 1"), record("SELECT 2")
        self.path.write_bytes(first + second)
        instance = self.make_monitor(initial_bytes=len(second) + 7)
        instance.poll()
        self.assertEqual(len(instance.snapshot()["events"]), 1)
        self.assertEqual(instance.raw_event(1)["sql"], "SELECT 2")
        exact = self.make_monitor(initial_bytes=len(second))
        exact.poll()
        self.assertEqual(exact.raw_event(1)["sql"], "SELECT 2")
        whole = self.make_monitor(initial_bytes=len(first + second))
        whole.poll()
        self.assertEqual(len(whole.snapshot()["events"]), 2)

    def test_initial_zero_and_partial_eof(self):
        self.path.write_bytes(record("SELECT 1")[:-1])
        instance = self.make_monitor(initial_bytes=0)
        instance.poll()
        self.append(b"\n" + record("SELECT 2"))
        instance.poll()
        self.assertEqual(len(instance.snapshot()["events"]), 1)
        self.assertEqual(instance.raw_event(1)["sql"], "SELECT 2")

    def test_missing_retry_does_not_create_file(self):
        instance = self.make_monitor()
        instance.poll()
        self.assertFalse(self.path.exists())
        self.assertFalse(instance.snapshot()["status"]["exists"])
        self.assertIn("missing", instance.snapshot()["status"]["error"])
        self.path.write_bytes(record())
        instance.poll()
        self.assertEqual(instance.snapshot()["status"]["error"], "")
        self.assertTrue(instance.snapshot()["status"]["exists"])
        self.assertEqual(len(instance.snapshot()["events"]), 1)

    def test_nonregular_source_and_permission_failure(self):
        self.path.mkdir()
        instance = self.make_monitor()
        instance.poll()
        self.assertIn("Cannot read", instance.snapshot()["status"]["error"])
        self.path.rmdir()
        self.path.write_bytes(record())
        with patch.object(monitor.os, "open", side_effect=PermissionError):
            instance.poll()
        self.assertIn("Cannot read", instance.snapshot()["status"]["error"])
        instance.poll()
        self.assertEqual(len(instance.snapshot()["events"]), 1)

    def test_truncation_resets_incomplete_line(self):
        self.path.write_bytes(record("SELECT 'old value'") + b'{"msg":"SQL Query",')
        instance = self.make_monitor()
        instance.poll()
        self.path.write_bytes(record("SELECT 2"))
        instance.poll()
        snapshot = instance.snapshot()
        self.assertEqual(snapshot["status"]["rotations"], 1)
        self.assertEqual(snapshot["status"]["parse_errors"], 0)
        self.assertEqual(instance.raw_event(2)["sql"], "SELECT 2")

    def test_same_inode_fast_regrow(self):
        self.path.write_bytes(record("SELECT 1"))
        inode = self.path.stat().st_ino
        instance = self.make_monitor()
        instance.poll()
        self.path.write_bytes(record("SELECT 22222222") + record("SELECT 3333333"))
        self.assertEqual(self.path.stat().st_ino, inode)
        instance.poll()
        self.assertEqual(instance.snapshot()["status"]["rotations"], 1)
        self.assertEqual(len(instance.snapshot()["events"]), 3)
        self.assertEqual(instance.raw_event(2)["sql"], "SELECT 22222222")

    def test_inode_rotation_and_recreate(self):
        self.path.write_bytes(record("SELECT 1"))
        instance = self.make_monitor()
        instance.poll()
        self.path.rename(self.path.with_suffix(".old"))
        instance.poll()
        self.assertFalse(instance.snapshot()["status"]["exists"])
        self.path.write_bytes(record("SELECT 2"))
        instance.poll()
        self.assertEqual(instance.snapshot()["status"]["rotations"], 1)
        self.assertEqual(instance.raw_event(2)["sql"], "SELECT 2")

    def test_retention_drops_raw_and_ids_do_not_repeat(self):
        self.path.write_bytes(b"".join(record("SELECT %d" % i) for i in range(5)))
        instance = self.make_monitor(max_events=2)
        instance.poll()
        self.assertEqual([event["id"] for event in instance.snapshot()["events"]], [4, 5])
        self.assertEqual(instance.snapshot()["status"]["dropped_events"], 3)
        self.assertIsNone(instance.raw_event(1))
        self.assertEqual(len(instance.raw), 2)
        raw = instance.raw_event(4)
        raw["sql"] = "mutated"
        self.assertNotEqual(instance.raw_event(4)["sql"], "mutated")

    def test_oversize_discarded_while_streaming_and_recovery(self):
        self.path.write_bytes(b"x" * (monitor.MAX_LINE + 37))
        instance = self.make_monitor()
        instance.poll()
        self.assertEqual(instance.snapshot()["status"]["oversize_lines"], 1)
        self.assertEqual(len(instance.pending), 0)
        self.append(b"more discarded\n" + record("SELECT 1"))
        instance.poll()
        snapshot = instance.snapshot()
        self.assertEqual(snapshot["status"]["oversize_lines"], 1)
        self.assertEqual(snapshot["status"]["lines_seen"], 2)
        self.assertEqual(len(snapshot["events"]), 1)
        self.assertEqual(snapshot["status"]["parse_errors"], 0)

    def test_exact_line_limit(self):
        self.path.write_bytes(b" " * monitor.MAX_LINE + b"\n" + record())
        instance = self.make_monitor()
        instance.poll()
        self.assertEqual(instance.snapshot()["status"]["oversize_lines"], 0)
        self.assertEqual(instance.snapshot()["status"]["ignored_lines"], 1)
        self.assertEqual(len(instance.snapshot()["events"]), 1)

    def test_ignored_and_parse_errors_never_store_payloads(self):
        self.path.write_bytes(b'{"msg":"request", "body":"private-request"}\n' + b'{"msg":"SQL Query",\n' + b'"sql":"SELECT 1"}\n' + record())
        instance = self.make_monitor()
        instance.poll()
        snapshot = instance.snapshot()
        self.assertEqual(snapshot["status"]["parse_errors"], 1)
        self.assertEqual(snapshot["status"]["ignored_lines"], 2)
        self.assertEqual(len(snapshot["events"]), 1)
        self.assertNotIn("private-request", json.dumps(snapshot))
        self.assertNotIn("private-request", json.dumps(instance.raw))

    def test_compact_multiline_prefix_is_conservatively_truncated(self):
        instance = self.make_monitor()
        for sql in ("SELECT col7", "SELECT col7 FROM t", "SELECT 1;"):
            with self.subTest(sql=sql):
                line = "[t] DEBUG [svc] [] caller.go:9 - SQL Query | sql=" + sql
                instance.consume((line + '\nWHERE name="continuation-secret"\n').encode())
                event = instance.snapshot()["events"][-1]
                self.assertTrue(event["truncated"])
                self.assertEqual(event["source"], "caller.go:9")
                self.assertEqual(instance.raw_event(event["id"])["sql"], sql)
                self.assertNotIn("continuation-secret", json.dumps(instance.snapshot()))
        instance.consume(record("SELECT col7 FROM t"))
        snapshot = instance.snapshot()
        self.assertEqual(len(snapshot["events"]), 4)
        self.assertFalse(snapshot["events"][-1]["truncated"])
        self.assertEqual(snapshot["status"]["ignored_lines"], 3)
        self.assertEqual(snapshot["status"]["parse_errors"], 0)

    def test_small_chunk_utf8_and_json_buffering(self):
        self.path.write_bytes(record("SELECT 'µ-secret'\nFROM t"))
        instance = self.make_monitor()
        with patch.object(monitor, "READ_CHUNK", 7):
            for _ in range(4):
                instance.poll()
        self.assertEqual(instance.raw_event(1)["sql"], "SELECT 'µ-secret'\nFROM t")
        self.assertEqual(instance.snapshot()["status"]["parse_errors"], 0)

    def test_daemon_thread_and_shutdown(self):
        self.path.write_bytes(record())
        instance = self.make_monitor()
        ready = threading.Event()
        original = instance.poll
        def poll():
            original()
            ready.set()
        with patch.object(instance, "poll", side_effect=poll):
            instance.start()
            self.assertTrue(ready.wait(2))
            self.assertTrue(instance.thread.daemon)
            instance.close()
        self.assertFalse(instance.thread.is_alive())
        self.assertEqual(len(instance.snapshot()["events"]), 1)

    def test_invalid_capacity(self):
        for capacity in (0, -1, 2001):
            with self.assertRaises(ValueError):
                self.make_monitor(max_events=capacity)
        with self.assertRaises(ValueError):
            self.make_monitor(initial_bytes=-1)


class HTTPTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "service.log"
        self.path.write_bytes(record("SELECT 'raw-only-secret'", msg="SQL Error", error="raw-only-error"))
        self.monitor = monitor.Monitor(self.path, max_events=1)
        self.addCleanup(self.monitor.close)
        self.monitor.poll()
        self.server = monitor.MonitorServer(self.monitor, 0)
        self.assertEqual(self.server.server_address[0], "127.0.0.1")
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop_server)
        self.host = "127.0.0.1:%d" % self.server.server_port

    def stop_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)

    def request(self, path="/api/state", method="GET", headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=2)
        try:
            connection.request(method, path, headers=headers or {})
            response = connection.getresponse()
            return response.status, dict(response.getheaders()), response.read()
        finally:
            connection.close()

    def test_state_contract_and_security_headers(self):
        code, headers, data = self.request()
        self.assertEqual(code, 200)
        snapshot = json.loads(data)
        self.assertEqual(set(snapshot), {"status", "events"})
        self.assertEqual(set(snapshot["status"]), {"path", "exists", "bytes_read", "lines_seen", "ignored_lines", "parse_errors", "rotations", "dropped_events", "oversize_lines", "started_at", "last_read", "capacity", "error"})
        self.assertEqual(set(snapshot["events"][0]), {"id", "time", "received_at", "level", "kind", "sql", "fingerprint", "elapsed_ms", "rows", "source", "trace_id", "span_id", "error", "truncated"})
        self.assertNotIn(b"raw-only-secret", data)
        self.assertNotIn(b"raw-only-error", data)
        self.assertEqual(headers["Cache-Control"], "no-store")
        self.assertIn("default-src 'none'", headers["Content-Security-Policy"])
        self.assertIn("'unsafe-inline'", headers["Content-Security-Policy"])
        self.assertNotIn("Access-Control-Allow-Origin", headers)
        self.assertEqual(headers["X-Content-Type-Options"], "nosniff")

    def test_raw_opt_in_and_eviction(self):
        self.assertEqual(self.request("/api/event/1")[0], 404)
        self.assertEqual(self.request("/api/event/1?raw=0")[0], 404)
        code, _, data = self.request("/api/event/1?raw=1")
        self.assertEqual(code, 200)
        self.assertEqual(json.loads(data), {"sql": "SELECT 'raw-only-secret'", "error": "raw-only-error"})
        with self.path.open("ab") as file:
            file.write(record("SELECT 2"))
        self.monitor.poll()
        code, headers, data = self.request("/api/event/1?raw=1")
        self.assertEqual(code, 404)
        self.assertIn("application/json", headers["Content-Type"])
        self.assertEqual(json.loads(data), {"error": "Event not found"})

    def test_host_origin_and_fetch_checks(self):
        for headers in ({"Host": "evil.example"}, {"Host": "localhost:9"}, {"Host": "127.0.0.1.evil:%d" % self.server.server_port},
                        {"Origin": "http://evil.example"}, {"Origin": "null"}, {"Origin": "https://" + self.host},
                        {"Origin": "http://localhost:%d" % self.server.server_port}, {"Sec-Fetch-Site": "cross-site"}):
            with self.subTest(headers=headers):
                self.assertEqual(self.request(headers=headers)[0], 403)
                self.assertEqual(self.request("/api/event/1?raw=1", headers=headers)[0], 403)
        self.assertEqual(self.request(headers={"Host": self.host, "Origin": "http://" + self.host})[0], 200)
        localhost = "localhost:%d" % self.server.server_port
        self.assertEqual(self.request(headers={"Host": localhost, "Origin": "http://" + localhost})[0], 200)

    def test_duplicate_or_missing_host_and_origin(self):
        for hosts, origins in (([], []), ([self.host, self.host], []), ([self.host], ["http://" + self.host] * 2)):
            connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=2)
            try:
                connection.putrequest("GET", "/api/state", skip_host=True)
                for host in hosts:
                    connection.putheader("Host", host)
                for origin in origins:
                    connection.putheader("Origin", origin)
                connection.endheaders()
                self.assertEqual(connection.getresponse().status, 403)
            finally:
                connection.close()

    def test_read_only_no_arbitrary_files_or_log_changes(self):
        before = self.path.read_bytes()
        for method in ("POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD", "TRACE", "CONNECT"):
            with self.subTest(method=method):
                self.assertEqual(self.request(method=method)[0], 405)
        for path in ("/monitor.py", "/index.html", "/../service.log", "/%2e%2e/service.log", "/api/state?raw=1", "/api/state?log=/etc/passwd", "http://" + self.host + "/api/state"):
            with self.subTest(path=path):
                code, headers, data = self.request(path)
                self.assertEqual(code, 404)
                self.assertIn("application/json", headers["Content-Type"])
                self.assertIn("error", json.loads(data))
        self.assertEqual(self.path.read_bytes(), before)

    def test_root_serves_only_adjacent_index_contract(self):
        self.assertEqual(self.server.index_path, Path(monitor.__file__).resolve().with_name("index.html"))
        fixture = Path(self.temp.name) / "fixture.html"
        fixture.write_text("<!doctype html><title>Test UI</title>", encoding="utf-8")
        with patch.object(self.server, "index_path", fixture):
            code, headers, data = self.request("/")
            self.assertEqual(code, 200)
            self.assertIn("text/html", headers["Content-Type"])
            self.assertEqual(data, fixture.read_bytes())
        with patch.object(self.server, "index_path", fixture.with_name("missing.html")):
            self.assertEqual(self.request("/")[0], 404)

    def test_concurrent_snapshots_and_raw_reads(self):
        errors = []
        def read():
            try:
                for _ in range(10):
                    self.assertEqual(self.request()[0], 200)
                    self.assertEqual(self.request("/api/event/1?raw=1")[0], 200)
            except Exception as error:
                errors.append(error)
        threads = [threading.Thread(target=read) for _ in range(4)]
        for thread in threads:
            thread.start()
        for _ in range(10):
            self.monitor.poll()
        for thread in threads:
            thread.join(5)
            self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])


class CLITests(unittest.TestCase):
    def test_help_and_validation_without_opening_live_log(self):
        script = str(Path(monitor.__file__).resolve())
        environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        help_result = subprocess.run([sys.executable, "-B", script, "--help"], capture_output=True, env=environment, timeout=5)
        self.assertEqual(help_result.returncode, 0)
        self.assertIn(b"--initial-bytes", help_result.stdout)
        for args in (("--max-events", "2001"), ("--port", "0"), ("--initial-bytes", "-1")):
            result = subprocess.run([sys.executable, "-B", script, *args], capture_output=True, env=environment, timeout=5)
            self.assertEqual(result.returncode, 2)


if __name__ == "__main__":
    unittest.main()
