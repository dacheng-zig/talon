"""Real TLS configured header windows, exact boundaries and pipelined plaintext."""
import ssl
import unittest

import test_tls_server as tls_cases


@unittest.skipUnless(tls_cases.PROBE.exists(), 'optional TLS probe not built')
class TlsReaderWindowTests(unittest.TestCase):
    start = tls_cases.ServerTlsTests.start
    finish = tls_cases.ServerTlsTests.finish
    connect = tls_cases.ServerTlsTests.connect

    @classmethod
    def setUpClass(cls):
        tls_cases.ServerTlsTests.setUpClass.__func__(cls)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def check_window(self, version, large):
        limit = 65536 if large else 16384
        port = self.start(duration=1500, mode='wide-head' if large else 'alpn-disabled')
        prefix = b'GET / HTTP/1.1\r\nHost: localhost\r\nX-Pad: '
        request = prefix + b'x' * (limit - len(prefix) - 4) + b'\r\n\r\n'
        self.assertEqual(len(request), limit)
        with self.connect(port, version, strict=True) as stream:
            # The second head must remain valid across SSL refill and rebasing.
            stream.sendall(request + b'GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n')
            raw = bytearray()
            while data := stream.recv(4096): raw.extend(data)
        self.assertEqual(raw.count(b'HTTP/1.1 200 OK\r\n'), 2)
        self.assertEqual(raw.count(b'secure talon\n'), 2)
        self.assertTrue(any(f'tls_reader_window={limit + 1024} max_header_size={limit}' in line for line in self.lines))

    def test_default_tls12(self): self.check_window(ssl.TLSVersion.TLSv1_2, False)
    def test_default_tls13(self): self.check_window(ssl.TLSVersion.TLSv1_3, False)
    def test_large_tls12(self): self.check_window(ssl.TLSVersion.TLSv1_2, True)
    def test_large_tls13(self): self.check_window(ssl.TLSVersion.TLSv1_3, True)

    def test_oversized_large_header_returns_431(self):
        port = self.start(duration=1500, mode='wide-head')
        with self.connect(port, ssl.TLSVersion.TLSv1_3, strict=True) as stream:
            stream.sendall(b'GET / HTTP/1.1\r\nHost: localhost\r\nX-Pad: ' + b'x' * 65536 + b'\r\n\r\n')
            raw = bytearray()
            while data := stream.recv(4096): raw.extend(data)
        self.assertTrue(raw.startswith(b'HTTP/1.1 431 '))


if __name__ == '__main__': unittest.main()
