#!/usr/bin/env python3
# ---------------------------------------------------------------------------
# HW3 - Web Servers and Synchronization : PUBLIC self-check tests
#
# This is a SUBSET of the test suite, provided so you can sanity-check your
# server before submitting. Passing all of these does NOT guarantee full
# marks: the graded suite contains many more (and stricter) tests.
#
# Usage (run from your project directory, after `make`):
#     python3 server.py                 # build + run all public tests here
#     python3 server.py -p /path/proj   # test a project in another folder
#     python3 server.py --no-build      # skip the `make` step
# ---------------------------------------------------------------------------
import time, os, http.client, random, threading, re, socket, sys

import toolspath
from testing import Test, BuildTest
from testing.test import Failure

# Test Registry
test_list = []


def parse_stat_headers(headers):
    parsed = {}
    for key, val in headers:
        val = val.lstrip(':').strip()
        try:
            val = float(val) if '.' in val else int(val)
        except ValueError:
            pass
        parsed[key] = val
    return parsed


def parse_stat_string(raw):
    """Parse a raw UDP stats payload into a dict.

    The server answers a UDP ping with the target thread's statistics as
    'Stat-Thread-X:: <value>' fields. Fields may be separated by newlines or
    by '#'. Values are coerced to int/float when possible.
    """
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode(errors="replace")
    parsed = {}
    for field in re.split(r"[\n#]+", raw):
        field = field.strip()
        if not field or "::" not in field:
            continue
        key, val = field.split("::", 1)
        key = key.strip()
        val = val.strip()
        try:
            val = float(val) if '.' in val else int(val)
        except ValueError:
            pass
        parsed[key] = val
    return parsed


def test(cls):
    test_list.append(cls)
    return cls


# --- Base Classes ---

class ArgTest(Test):
    timeout = 2

    def run_argtest(self, port=0, udp_port=0, threads=1, buffers=1, debug=0, n=None):
        if (port == 0):
            port = random.randint(5000, 10000)
        if (udp_port == 0):
            udp_port = random.randint(10001, 15000)
        self.log(f"Starting server on tcp port {port}, udp port {udp_port}")
        # CLI: ./server [tcp_port] [udp_port] [threads] [queue_size] [debug]
        args = ["server", str(port), str(udp_port), str(threads), str(buffers), str(debug)]
        if n is not None:
            args.append(str(n))

        serverProc = self.startexe(args)
        time.sleep(0.5)

        if (serverProc.poll() == None):
            serverProc.kill()
            self.fail("Server stayed alive with invalid arguments")
        else:
            self.log(f"Server exited as expected (Code: {serverProc.returncode})")

    def run_argtest_raw(self, server_args):
        """Launch the server with an explicit argument list (without the
        program name) and require that it exits (rejects the arguments)."""
        args = ["server"] + [str(a) for a in server_args]
        self.log(f"Starting server with raw args: {server_args}")
        serverProc = self.startexe(args)
        time.sleep(0.5)

        if (serverProc.poll() == None):
            serverProc.kill()
            self.fail(f"Server stayed alive with invalid arguments: {server_args}")
        else:
            self.log(f"Server exited as expected (Code: {serverProc.returncode})")


class ServerTest(Test):
    timeout = 30

    def quiet_get(self, conn, item):
        try:
            conn.request('GET', item)
            return True
        except Exception:
            self.fail('Unable to send GET request; server may have crashed.')
            return False

    def quiet_post(self, conn, item):
        try:
            conn.request('POST', item)
            return True
        except Exception as e:
            self.fail('unable to send POST request; server may have crashed.')
            return False

    def client_run(self, conn):
        try:
            response = conn.getresponse()
            msg = response.read()
            conn.close()
            if (len(msg) == 0):
                self.fail("missing body in response")
        except Exception as inst:
            self.fail("Client failed with error: " + str(inst))

    def run_server(self, threads, q_size, debug=0):
        for i in range(5):
            port = random.randint(5000, 10000)
            udp_port = random.randint(10001, 15000)
            self.log(f"Starting server on tcp port {port}, udp port {udp_port}")
            # CLI: ./server [tcp_port] [udp_port] [threads] [queue_size] [debug]
            args = ["server", str(port), str(udp_port), str(threads), str(q_size), str(debug)]

            serverProc = self.startexe(args)
            time.sleep(0.2)

            if serverProc.poll() is None:
                self.port = port
                self.udp_port = udp_port
                self.serverProc = serverProc
                return serverProc
        raise Failure("Could not start server after 5 attempts")

    def send_udp_ping(self, thread_id, attempts=3, timeout=2):
        """Ping the server's UDP channel for a thread's statistics.

        Wire format:
          - request : the thread id as plain ASCII bytes, e.g. b"1"
          - response: that thread's 'Stat-Thread-X:: <value>' fields

        Retries because UDP is unreliable; returns the parsed stats dict, or
        raises socket.timeout if no reply arrives across all attempts.
        """
        last_err = None
        for _ in range(attempts):
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(timeout)
            try:
                sock.sendto(str(thread_id).encode(), ("localhost", self.udp_port))
                data, _ = sock.recvfrom(4096)
                self.last_udp_raw = data
                return parse_stat_string(data)
            except socket.timeout as e:
                last_err = e
                continue
            finally:
                sock.close()
        raise last_err if last_err else socket.timeout("no UDP response")


# --- Argument Validation Tests ---

@test
class ArgBuffers(ArgTest):
    name = "argbuffers"
    description = "invalid number of buffers"

    def run(self):
        self.run_argtest(buffers=-1)
        self.done()


@test
class ArgPort(ArgTest):
    name = "argport"
    description = "invalid port number"

    def run(self):
        self.run_argtest(port=22)
        self.done()


@test
class ArgThreads(ArgTest):
    name = "argthreads"
    description = "invalid number of threads"

    def run(self):
        self.run_argtest(threads=-1)
        self.done()


@test
class ArgUdpPort(ArgTest):
    name = "argudpport"
    description = "invalid udp port number"

    def run(self):
        self.run_argtest(udp_port=22)
        self.done()


@test
class ArgUdpEqualsTcp(ArgTest):
    name = "argudpequalstcp"
    description = "udp port must differ from tcp port"

    def run(self):
        same = random.randint(5000, 10000)
        self.run_argtest(port=same, udp_port=same)
        self.done()


@test
class ArgCount(ArgTest):
    name = "argcount"
    description = "too few command-line arguments"

    def run(self):
        port = random.randint(5000, 10000)
        self.run_argtest_raw([port, port + 1])
        self.done()


# --- Concurrency: one locking test ---

@test
class Locks(ServerTest):
    name = "locks"
    threads = 8
    buffers = 16
    num_clients = 20
    loops = 5
    description = "sends many concurrent requests to test locking."
    requests = ["/home.html", "/output.cgi?0.3", "/favicon.ico"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.description += (f' server params: threads {self.threads}, Q_size {self.buffers}. '
                             f'{self.num_clients} clients each requesting {self.requests}, {self.loops} times')

    def many_reqs(self):
        for i in range(self.loops):
            for request in self.requests:
                if self.is_failed():
                    return
                conn = http.client.HTTPConnection("localhost", self.port, timeout=20)
                if self.quiet_get(conn, request):
                    self.client_run(conn)

    def run(self):
        serverProc = self.run_server(threads=self.threads, q_size=self.buffers)
        clients = [threading.Thread(target=self.many_reqs) for _ in range(self.num_clients)]
        for client in clients:
            client.start()
        for client in clients:
            client.join()

        if (serverProc.poll() != None):
            self.fail("server exited or crashed")
        else:
            serverProc.kill()
        self.done()


# --- Log structure ---

@test
class LogConsistency(ServerTest):
    name = "log_consistency"
    description = "Check if GET appends to the log and POST retrieves the full log string."
    threads, buffers = 2, 5

    def run(self):
        serverProc = self.run_server(self.threads, self.buffers)
        try:
            # Step 1: Multiple GETs to fill the log
            for i in range(3):
                conn = http.client.HTTPConnection("localhost", self.port)
                self.quiet_get(conn, "/home.html")
                conn.getresponse().read()

            # Step 2: POST to retrieve the log
            conn = http.client.HTTPConnection("localhost", self.port)
            self.quiet_post(conn, "/home.html")
            response = conn.getresponse()
            body = response.read().decode()

            occurrences = len(re.findall("Stat-Req-Arrival::", body))
            if occurrences < 3:
                self.fail(f"Log expected at least 3 entries, found {occurrences}")
        finally:
            serverProc.kill()
        self.done()


# --- UDP statistics channel ---

@test
class UdpStats(ServerTest):
    name = "udp_stats"
    description = "Ping the UDP port for a thread id and verify the returned thread-statistics dictionary."
    threads, buffers = 4, 8

    required_thread_stats = [
        "Stat-Thread-Id", "Stat-Thread-Count", "Stat-Thread-Static",
        "Stat-Thread-Dynamic", "Stat-Thread-Post",
    ]
    # The exact reply format the server is expected to produce.
    expected_format = (
        "Stat-Thread-Id:: <int>\n"
        "Stat-Thread-Count:: <int>\n"
        "Stat-Thread-Static:: <int>\n"
        "Stat-Thread-Dynamic:: <int>\n"
        "Stat-Thread-Post:: <int>"
    )

    def _format_error(self, problem, raw):
        """Build a failure message that shows the expected format and what the
        server actually sent, so the issue is easy to fix."""
        return (f"{problem}\n"
                f"--- Expected UDP reply format (integer values, '::' separator) ---\n"
                f"{self.expected_format}\n"
                f"--- Actual reply received ---\n"
                f"{raw!r}")

    def run(self):
        serverProc = self.run_server(self.threads, self.buffers)
        try:
            # Generate some HTTP traffic first so threads have handled requests.
            for _ in range(5):
                conn = http.client.HTTPConnection("localhost", self.port, timeout=10)
                if self.quiet_get(conn, "/home.html"):
                    conn.getresponse().read()
                conn.close()

            target_id = 1
            try:
                stats = self.send_udp_ping(target_id)
            except socket.timeout:
                self.fail(f"No UDP response on port {self.udp_port} for thread {target_id} (timed out).\n"
                          f"The server must reply to a UDP ping with:\n{self.expected_format}")
                self.done()
                return
            except Exception as e:
                self.fail(f"UDP ping failed: {e}")
                self.done()
                return

            raw = getattr(self, "last_udp_raw", b"")
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode(errors="replace")
            self.actual_output = f"UDP reply: {raw!r}"
            self.log(f"UDP stats for thread {target_id}: {stats}")

            if not stats:
                self.fail(self._format_error("UDP response was empty or could not be parsed.", raw))
                self.done()
                return

            # 1) All five per-thread statistics must be present.
            for key in self.required_thread_stats:
                if key not in stats:
                    self.fail(self._format_error(f"Missing statistic '{key}' in the UDP response.", raw))
                    self.done()
                    return

            # 2) Each value must be an integer (format '<key>:: <int>').
            for key in self.required_thread_stats:
                if not isinstance(stats[key], int):
                    self.fail(self._format_error(
                        f"Statistic '{key}' must be an integer, got {stats[key]!r}.", raw))
                    self.done()
                    return

            # 3) The reply must describe the thread we asked about.
            if stats["Stat-Thread-Id"] != target_id:
                self.fail(self._format_error(
                    f"Reply reported Stat-Thread-Id {stats['Stat-Thread-Id']}, expected {target_id}.", raw))
                self.done()
                return

            if serverProc.poll() is not None:
                self.fail("Server crashed during the UDP stats test.")
        finally:
            serverProc.kill()
        self.done()


def _make_server_test(name, description, run_func, timeout=20):
    attrs = {
        'name': name,
        'description': description,
        'timeout': timeout,
        'run': run_func,
    }
    return test(type(name, (ServerTest,), attrs))


def _make_arg_test(name, description, **kwargs):
    def run(self):
        self.run_argtest(**kwargs)
        self.done()
    attrs = {
        'name': name,
        'description': description,
        'run': run,
    }
    return test(type(name, (ArgTest,), attrs))


def _make_arg_test_raw(name, description, server_args):
    def run(self):
        self.run_argtest_raw(server_args)
        self.done()
    attrs = {
        'name': name,
        'description': description,
        'run': run,
    }
    return test(type(name, (ArgTest,), attrs))


def _http_request_test(name, description, path, method='GET', expected_status=200,
                       expected_contains=None, expected_content_type=None,
                       threads=2, q_size=5, expect_body=True, timeout=15):
    def run(self):
        serverProc = self.run_server(threads=threads, q_size=q_size)
        try:
            conn = http.client.HTTPConnection('localhost', self.port, timeout=15)
            if method == 'GET':
                if not self.quiet_get(conn, path):
                    return
            elif method == 'POST':
                if not self.quiet_post(conn, path):
                    return
            else:
                conn.request(method, path)
            response = conn.getresponse()
            body = response.read()
            conn.close()

            if response.status != expected_status:
                self.fail(f'Expected status {expected_status} but got {response.status}')
                return
            if expected_content_type is not None:
                content_type = response.getheader('Content-Type')
                if content_type is None:
                    self.fail(f'Expected Content-Type {expected_content_type} but got none')
                    return
                if not (content_type == expected_content_type or content_type.startswith(expected_content_type + ';') or content_type.startswith(expected_content_type + ',')):
                    self.fail(f'Expected Content-Type {expected_content_type} but got {content_type}')
                    return
            if expected_contains is not None:
                if expected_contains not in body.decode(errors='replace'):
                    self.fail(f"Response body did not contain expected text: {expected_contains}")
                    return
            if expect_body and len(body) == 0:
                self.fail('Expected non-empty body but response body was empty')
                return
        finally:
            serverProc.kill()
        self.done()
    return _make_server_test(name, description, run, timeout=timeout)


def _make_udp_ping_test(name, description, target_id, min_count=0, threads=4, q_size=8, timeout=20):
    def run(self):
        serverProc = self.run_server(threads=threads, q_size=q_size)
        try:
            for _ in range(5):
                conn = http.client.HTTPConnection('localhost', self.port, timeout=10)
                if self.quiet_get(conn, '/home.html'):
                    conn.getresponse().read()
                conn.close()
            stats = self.send_udp_ping(target_id)
            if stats['Stat-Thread-Id'] != target_id:
                self.fail(f'Stat-Thread-Id expected {target_id}, got {stats["Stat-Thread-Id"]}')
                return
            for key in ('Stat-Thread-Count', 'Stat-Thread-Static', 'Stat-Thread-Dynamic', 'Stat-Thread-Post'):
                if key not in stats:
                    self.fail(f'Missing UDP statistic {key}')
                    return
                if not isinstance(stats[key], int):
                    self.fail(f'UDP statistic {key} is not integer: {stats[key]!r}')
                    return
            if stats['Stat-Thread-Count'] < min_count:
                self.fail(f'Expected Stat-Thread-Count >= {min_count} but got {stats["Stat-Thread-Count"]}')
                return
        finally:
            serverProc.kill()
        self.done()
    return _make_server_test(name, description, run, timeout=timeout)


def _make_concurrent_post_test(name, description, get_requests=3, post_clients=5, threads=4, q_size=8, timeout=25):
    def run(self):
        serverProc = self.run_server(threads=threads, q_size=q_size)
        try:
            for _ in range(get_requests):
                conn = http.client.HTTPConnection('localhost', self.port, timeout=10)
                if self.quiet_get(conn, '/home.html'):
                    conn.getresponse().read()
                conn.close()

            failures = []
            def worker():
                try:
                    conn = http.client.HTTPConnection('localhost', self.port, timeout=15)
                    if self.quiet_post(conn, '/home.html'):
                        response = conn.getresponse()
                        body = response.read()
                        if response.status != 200:
                            failures.append(f'Status {response.status}')
                        elif b'Stat-Req-Arrival::' not in body:
                            failures.append('Missing log entries in POST response')
                    conn.close()
                except Exception as e:
                    failures.append(str(e))

            threads_list = [threading.Thread(target=worker) for _ in range(post_clients)]
            for t in threads_list:
                t.start()
            for t in threads_list:
                t.join()
            if failures:
                self.fail(' | '.join(failures))
        finally:
            serverProc.kill()
        self.done()
    return _make_server_test(name, description, run, timeout=timeout)


def _make_queue_blocking_test(name, description, slow_path, fast_path, threads=1, q_size=1, timeout=30):
    def run(self):
        serverProc = self.run_server(threads=threads, q_size=q_size)
        t1 = None
        t2 = None
        conn1 = None
        conn2 = None
        try:
            start = time.time()
            conn1 = http.client.HTTPConnection('localhost', self.port, timeout=30)
            conn2 = http.client.HTTPConnection('localhost', self.port, timeout=30)
            if self.quiet_get(conn1, slow_path):
                t1 = threading.Thread(target=lambda: conn1.getresponse().read())
                t1.start()
            if self.quiet_get(conn2, fast_path):
                t2 = threading.Thread(target=lambda: conn2.getresponse().read())
                t2.start()
            if t1:
                t1.join()
            if t2:
                t2.join()
            elapsed = time.time() - start
            if elapsed < 0.15:
                self.fail(f'Expected queue to block when full, but completed too quickly ({elapsed:.2f}s)')
                return
        finally:
            try:
                if conn1:
                    conn1.close()
                if conn2:
                    conn2.close()
            except:
                pass
            serverProc.kill()
        self.done()
    return _make_server_test(name, description, run, timeout=timeout)


def _make_stats_header_test(name, description, path, expected_keys, timeout=20):
    def run(self):
        serverProc = self.run_server(threads=2, q_size=5)
        try:
            conn = http.client.HTTPConnection('localhost', self.port, timeout=15)
            if not self.quiet_get(conn, path):
                return
            response = conn.getresponse()
            header_text = response.getheaders()
            body = response.read()
            conn.close()
            if response.status != 200:
                self.fail(f'Expected 200 but got {response.status}')
                return
            raw = '\r\n'.join(f'{k}: {v}' for k, v in header_text)
            for key in expected_keys:
                if key not in raw:
                    self.fail(f'Missing expected header field {key}')
                    return
        finally:
            serverProc.kill()
        self.done()
    return _make_server_test(name, description, run, timeout=timeout)


def _make_multi_get_test(name, description, request_paths, expected_contains, timeout=25):
    def run(self):
        serverProc = self.run_server(threads=4, q_size=8)
        try:
            for path in request_paths:
                conn = http.client.HTTPConnection('localhost', self.port, timeout=15)
                if self.quiet_get(conn, path):
                    response = conn.getresponse()
                    body = response.read()
                    conn.close()
                    if response.status != 200:
                        self.fail(f'Expected 200 for {path} but got {response.status}')
                        return
            conn = http.client.HTTPConnection('localhost', self.port, timeout=15)
            if self.quiet_post(conn, '/home.html'):
                response = conn.getresponse()
                body = response.read().decode(errors='replace')
                conn.close()
                if expected_contains not in body:
                    self.fail(f'Missing expected log text {expected_contains}')
                    return
        finally:
            serverProc.kill()
        self.done()
    return _make_server_test(name, description, run, timeout=timeout)


def _make_writer_priority_test(name, description, post_clients=8, threads=4, q_size=8, timeout=30):
    def run(self):
        serverProc = self.run_server(threads=threads, q_size=q_size)
        try:
            failures = []
            def post_worker():
                try:
                    conn = http.client.HTTPConnection('localhost', self.port, timeout=15)
                    if self.quiet_post(conn, '/home.html'):
                        response = conn.getresponse()
                        response.read()
                    conn.close()
                except Exception as e:
                    failures.append(str(e))

            post_threads = [threading.Thread(target=post_worker) for _ in range(post_clients)]
            for t in post_threads:
                t.start()
            time.sleep(0.2)
            conn = http.client.HTTPConnection('localhost', self.port, timeout=15)
            if self.quiet_get(conn, '/home.html'):
                response = conn.getresponse()
                response.read()
                if response.status != 200:
                    failures.append(f'GET status {response.status}')
                conn.close()
            for t in post_threads:
                t.join()
            if failures:
                self.fail(' | '.join(failures))
        finally:
            serverProc.kill()
        self.done()
    return _make_server_test(name, description, run, timeout=timeout)


# Detailed coverage tests generated from assignment requirements
_make_arg_test('ArgThreadsZero', 'invalid thread count of zero', threads=0)
_make_arg_test('ArgBuffersZero', 'invalid queue size of zero', buffers=0)
_make_arg_test('ArgPort1024', 'invalid tcp port number below 2001', port=1024)
_make_arg_test('ArgUdpPort1024', 'invalid udp port number below 2001', udp_port=1024)
_make_arg_test('ArgNegativeThreads', 'invalid negative number of threads', threads=-5)
_make_arg_test('ArgNegativeBuffers', 'invalid negative queue size', buffers=-10)
_make_arg_test('ArgTcpUdpSameLarge', 'invalid when tcp and udp ports are equal', port=5005, udp_port=5005)
_make_arg_test_raw('ArgTooFewArgs', 'missing required arguments', [5000, 5001])

_http_request_test('GetHomeHtml', 'GET /home.html returns a valid HTML page', '/home.html', expected_contains='<html', expected_content_type='text/html')
_http_request_test('GetFavicon', 'GET /favicon.ico returns binary content', '/favicon.ico', expected_status=200, expected_content_type='image/gif')
_http_request_test('GetRootPage', 'GET / returns the home page', '/', expected_contains='<html', expected_content_type='text/html')
_http_request_test('GetDotDotPath', 'GET /../home.html is sanitized to home page', '/../home.html', expected_contains='<html', expected_content_type='text/html')
_http_request_test('GetCgiOutput', 'GET /output.cgi returns CGI output', '/output.cgi?0.01', expected_contains='Welcome to the CGI program', expect_body=True)
_http_request_test('GetCgiQueryArgs', 'GET /output.cgi passes query args to CGI', '/output.cgi?0.01', expected_contains='spun for', expect_body=True)
_http_request_test('GetOutputCgiWithQueryDecimal', 'GET /output.cgi?0.05 returns CGI output with query parameter', '/output.cgi?0.05', expected_contains='spun for', expect_body=True)
_http_request_test('GetDynamicPathNoQuery', 'GET /output.cgi with no query still executes', '/output.cgi', expected_contains='Welcome to the CGI program', expect_body=True)
_http_request_test('GetStaticContentLength', 'GET /home.html has matching Content-Length', '/home.html', expected_contains='<html', expected_content_type='text/html')
_http_request_test('GetHomeHtmlSecondTime', 'GET /home.html twice still succeeds', '/home.html', expected_contains='<html', expected_content_type='text/html')
_http_request_test('GetFaviconSecondTime', 'GET /favicon.ico twice still succeeds', '/favicon.ico', expected_status=200, expected_content_type='image/gif')
_http_request_test('PostReturnsLogAfterOneGet', 'POST returns log after one GET', '/home.html', method='POST', expected_status=200, expected_contains='Stat-Req-Arrival::', expected_content_type='text/plain')
_http_request_test('PostReturnsLogAfterMultipleGets', 'POST returns log after multiple GETs', '/home.html', method='POST', expected_status=200, expected_contains='Stat-Req-Arrival::', expected_content_type='text/plain')
_http_request_test('UnsupportedMethodPut', 'PUT returns 501 Not Implemented', '/home.html', method='PUT', expected_status=501, expect_body=False)
_http_request_test('MissingFileNotFound', 'GET missing file returns 404', '/doesnotexist.html', expected_status=404, expect_body=True)
_http_request_test('PostContentTypePlain', 'POST response Content-Type is text/plain', '/home.html', method='POST', expected_content_type='text/plain')
_http_request_test('PostAfterNoGet', 'POST after no GET returns an empty log payload', '/home.html', method='POST', expected_status=200, expect_body=True)
_http_request_test('StatsHeaderPresence', 'GET response includes internal stat headers', '/home.html', expected_contains='Stat-Thread-Id::', expected_content_type='text/html')
_http_request_test('StaticFileTypeHtml', 'GET /home.html returns HTML file type', '/home.html', expected_content_type='text/html')

_make_udp_ping_test('UdpStatsThread0', 'UDP ping returns stats for thread 0', 0, min_count=0)
_make_udp_ping_test('UdpStatsThread1', 'UDP ping returns stats for thread 1', 1, min_count=0)
_make_udp_ping_test('UdpStatsThread2', 'UDP ping returns stats for thread 2', 2, min_count=0)
_make_udp_ping_test('UdpStatsThread3', 'UDP ping returns stats for thread 3', 3, min_count=0)
_make_udp_ping_test('UdpStatsThread0AfterLoad', 'UDP thread 0 stats after additional traffic', 0, min_count=1)
_make_udp_ping_test('UdpStatsThread1AfterDynamic', 'UDP thread 1 stats after dynamic content', 1, min_count=1)

_make_concurrent_post_test('PostReadersConcurrent', 'Multiple POST requests can read the log concurrently', get_requests=3, post_clients=8)
_make_concurrent_post_test('PostReadersHighConcurrency', 'High concurrency POST readers still succeed', get_requests=5, post_clients=10, threads=6, q_size=8, timeout=30)
_make_concurrent_post_test('PostReadersAfterMixedGets', 'POST requests after mixed GET traffic still read log', get_requests=3, post_clients=6, threads=5, q_size=8, timeout=30)

_make_writer_priority_test('WriterPriorityUnderLoad', 'A GET writer can still complete while many POST readers are active', post_clients=10, threads=6, q_size=10, timeout=35)


def _make_file_load_test(name, description, path, load_clients=10, threads=8, q_size=16, timeout=30):
    def run(self):
        serverProc = self.run_server(threads=threads, q_size=q_size)
        try:
            failures = []
            def worker():
                try:
                    conn = http.client.HTTPConnection('localhost', self.port, timeout=20)
                    if self.quiet_get(conn, path):
                        response = conn.getresponse()
                        body = response.read()
                        if response.status != 200:
                            failures.append(f'{path} status {response.status}')
                    conn.close()
                except Exception as e:
                    failures.append(str(e))

            pool = [threading.Thread(target=worker) for _ in range(load_clients)]
            for t in pool:
                t.start()
            for t in pool:
                t.join()
            if failures:
                self.fail(' | '.join(failures))
        finally:
            serverProc.kill()
        self.done()
    return _make_server_test(name, description, run, timeout=timeout)


_make_file_load_test('SimultaneousHomeLoads', 'Many concurrent GET /home.html requests succeed under load', '/home.html', load_clients=20)
_make_file_load_test('SimultaneousFaviconLoads', 'Many concurrent GET /favicon.ico requests succeed under load', '/favicon.ico', load_clients=15)
_make_file_load_test('SimultaneousDynamicLoads', 'Many concurrent dynamic GET requests succeed under load', '/output.cgi?0.01', load_clients=12)


def _make_log_match_test(name, description, get_paths, expected_log_entries, timeout=30):
    def run(self):
        serverProc = self.run_server(threads=4, q_size=10)
        try:
            for path in get_paths:
                conn = http.client.HTTPConnection('localhost', self.port, timeout=15)
                if self.quiet_get(conn, path):
                    response = conn.getresponse()
                    response.read()
                conn.close()
            conn = http.client.HTTPConnection('localhost', self.port, timeout=15)
            if self.quiet_post(conn, '/home.html'):
                response = conn.getresponse()
                body = response.read().decode(errors='replace')
                conn.close()
                for expected in expected_log_entries:
                    if expected not in body:
                        self.fail(f'Expected log entry {expected} missing')
                        return
        finally:
            serverProc.kill()
        self.done()
    return _make_server_test(name, description, run, timeout=timeout)


_make_log_match_test('LogContainsStaticAndDynamic', 'POST log contains entries from both static and dynamic GETs', ['/home.html', '/output.cgi?0.01'], ['Stat-Req-Arrival::', 'Stat-Thread-Id::'])
_make_log_match_test('LogContainsRepeatedGets', 'POST log contains entries for repeated GETs', ['/home.html', '/home.html', '/home.html'], ['Stat-Req-Arrival::'])


def _make_bulk_udp_test(num_threads=4):
    for tid in range(num_threads):
        _make_udp_ping_test(f'UdpStatsThread{tid}Bulk', f'UDP stats for thread {tid} after traffic', tid, min_count=0, threads=num_threads, q_size=8, timeout=20)

_make_bulk_udp_test(4)


# ---------------------------------------------------------------------------
# Standalone runner (no grader). Builds with `make`, then runs each public
# test in an isolated subprocess with a timeout, and prints a summary.
# ---------------------------------------------------------------------------
def _main():
    import argparse, multiprocessing, queue as _queue
    from testing.test import run_test, TestResult

    try:
        multiprocessing.set_start_method("fork", force=True)
    except (ValueError, RuntimeError):
        pass

    ap = argparse.ArgumentParser(description="HW3 public self-check tests")
    ap.add_argument("-p", "--project-path", default=".", help="Project directory to test")
    ap.add_argument("-b", "--no-build", action="store_false", dest="build", default=True,
                    help="Skip the 'make' build step")
    ap.add_argument("-f", "--factor", type=float, default=1.5, help="Timeout multiplier")
    opts = ap.parse_args()

    project = os.path.abspath(opts.project_path)
    suite = ([BuildTest] if opts.build else []) + test_list

    passed = 0
    counted = 0
    failures = []
    for cls in suite:
        t = cls(project_path=project, log=sys.stdout, max_score=1)

        result_q = multiprocessing.Queue()
        p = multiprocessing.Process(target=run_test, args=(t, result_q))
        p.start()
        timeout = (t.timeout or 30) * opts.factor
        try:
            result = result_q.get(block=True, timeout=timeout)
        except _queue.Empty:
            result = TestResult(t)
            result.state = Test.FAILED
            result.comment = f"TIMEOUT after {timeout:.0f}s"
        finally:
            if p.is_alive():
                p.terminate()
            result_q.close()

        ok = (result.state == Test.PASSED)
        if not ok:
            failures.append((t.name, t.description, result.comment))
            print("\n" + "=" * 60)
            print(f" {t.name}: {t.description}")
            print("=" * 60)
            print(f"  >>> FAIL" + (f"  ({result.comment})" if result.comment else ""))

        if t.name == "build":
            if not ok:
                print("\nBuild failed - fix compilation before running the tests.")
                return 1
            continue

        counted += 1
        if ok:
            passed += 1

    if failures:
        print("\n" + "=" * 60)
        print(f" RESULT: {passed}/{counted} public tests passed")
        print(f" FAILED: {len(failures)}")
        print("=" * 60)
        print("Note: this is only a subset. The graded suite is larger and stricter.")
    return 0 if passed == counted else 1


if __name__ == "__main__":
    sys.exit(_main())
