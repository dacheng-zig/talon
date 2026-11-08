"""Independent deterministic HTTP/1.1 or TLS peer for the network benchmark."""

import argparse
import http.server
import socket
import ssl
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8081)
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--body-bytes", type=int, default=17)
    parser.add_argument("--status", type=int, default=200)
    parser.add_argument("--cert")
    parser.add_argument("--key")
    parser.add_argument("--tls-version", choices=("1.2", "1.3"))
    parser.add_argument("--expect-sni")
    parser.add_argument("--expect-host")
    parser.add_argument("--delay-ms", type=int, default=0)
    args = parser.parse_args()
    if not 0 <= args.port <= 65535 or not 0 <= args.body_bytes <= 16 * 1024 * 1024:
        parser.error("port/body size out of range")
    if not 100 <= args.status <= 599 or bool(args.cert) != bool(args.key):
        parser.error("invalid status or incomplete cert/key pair")
    if not 0 <= args.delay_ms <= 60000:
        parser.error("delay out of range")
    body = b"x" * args.body_bytes

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def setup(self):
            super().setup()
            self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        def do_GET(self):
            if args.delay_ms:
                time.sleep(args.delay_ms / 1000)
            status = args.status
            if args.expect_host is not None and self.headers.get("Host") != args.expect_host:
                status = 400
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Type", "application/octet-stream")
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass  # Expected when the benchmark rejects a response.

        def log_message(self, *_):
            pass

    class FixtureServer(http.server.ThreadingHTTPServer):
        # Python's default backlog of five is too small for a worker burst.
        request_queue_size = 128
        address_family = socket.AF_INET6 if ":" in args.bind else socket.AF_INET

    server = FixtureServer((args.bind, args.port), Handler)
    if args.cert:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(args.cert, args.key)
        if args.tls_version:
            version = ssl.TLSVersion.TLSv1_2 if args.tls_version == "1.2" else ssl.TLSVersion.TLSv1_3
            context.minimum_version = context.maximum_version = version
        if args.expect_sni is not None:
            expected = None if args.expect_sni == "none" else args.expect_sni

            def check_sni(_socket, name, _context):
                if name != expected:
                    return ssl.ALERT_DESCRIPTION_UNRECOGNIZED_NAME

            context.set_servername_callback(check_sni)
        server.socket = context.wrap_socket(server.socket, server_side=True)
    scheme = "https" if args.cert else "http"
    host = f"[{args.bind}]" if ":" in args.bind else args.bind
    print(f"fixture={scheme}://{host}:{server.server_port}/ body_bytes={len(body)} status={args.status} delay_ms={args.delay_ms}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
