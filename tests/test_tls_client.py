"""Local DNS/IP certificate acceptance through the public Talon TLS client."""
from pathlib import Path
import re
import select
import socket
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent.parent
PROBE = ROOT / 'zig-out/bin/tls_probe'


@unittest.skipUnless(PROBE.exists(), 'TLS client probe not built')
class ClientIdentityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.directory = Path(cls.temp.name)
        cls.ca = cls.directory / 'ca.pem'
        cls.ca_key = cls.directory / 'ca-key.pem'
        cls.key = cls.directory / 'key.pem'
        cls.csr = cls.directory / 'leaf.csr'
        cls.openssl('req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
                    '-subj', '/CN=Talon test CA', '-addext', 'basicConstraints=critical,CA:TRUE',
                    '-keyout', cls.ca_key, '-out', cls.ca)
        cls.openssl('req', '-new', '-newkey', 'rsa:2048', '-nodes', '-subj', '/CN=127.0.0.1',
                    '-keyout', cls.key, '-out', cls.csr)
        cls.certificates = {}
        for name in ['ip', 'ip-only', 'wrong-ip', 'dns', 'cn']:
            cert = cls.directory / f'{name}.pem'
            cls.openssl('x509', '-req', '-in', cls.csr, '-CA', cls.ca, '-CAkey', cls.ca_key,
                        '-set_serial', str(len(cls.certificates) + 1), '-days', '1',
                        '-extfile', ROOT / f'tests/fixtures/tls/{name}.ext', '-out', cert)
            cls.certificates[name] = cert

    @classmethod
    def openssl(cls, *args):
        subprocess.run(['openssl', *map(str, args)], check=True, capture_output=True, timeout=15)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def check_identity(self, name, host, version, expected, bind='127.0.0.1'):
        process = subprocess.Popen([
            sys.executable, str(ROOT / 'bench/fixture.py'), '--port', '0', '--bind', bind,
            '--cert', str(self.certificates[name]), '--key', str(self.key), '--tls-version', version,
            '--expect-sni', 'localhost' if host == 'localhost' else 'none',
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.assertTrue(select.select([process.stdout], [], [], 10)[0], "TLS fixture startup timeout")
            ready = process.stdout.readline()
            match = re.search(r':(\d+)/', ready)
            self.assertIsNotNone(match, ready)
            url_host = f'[{host}]' if ':' in host else host
            result = subprocess.run([str(PROBE), f'https://{url_host}:{match[1]}/', str(self.ca), expected],
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn(f'result={expected} requested_bytes_after_deinit=0 active=0 waiting=0', result.stderr)
        finally:
            process.terminate()
            process.communicate(timeout=5)

    def test_dns_and_ip_san_are_distinct_in_tls12_and_tls13(self):
        for version in ['1.2', '1.3']:
            for name, host, expected in [
                ('ip', '127.0.0.1', 'ok'),
                ('ip-only', '127.0.0.1', 'ok'),
                ('wrong-ip', '127.0.0.1', 'CertificateHostMismatch'),
                ('dns', '127.0.0.1', 'CertificateHostMismatch'),
                ('cn', '127.0.0.1', 'CertificateHostMismatch'),
                ('ip', 'localhost', 'ok'),
                ('dns', 'localhost', 'ok'),
            ]:
                with self.subTest(version=version, certificate=name, host=host):
                    self.check_identity(name, host, version, expected)

    def test_ipv6_san_uses_binary_identity_without_sni(self):
        try:
            with socket.socket(socket.AF_INET6) as sock:
                sock.bind(('::1', 0))
        except OSError:
            self.skipTest('IPv6 loopback unavailable')
        for version in ['1.2', '1.3']:
            for name, expected in [('ip', 'ok'), ('wrong-ip', 'CertificateHostMismatch')]:
                with self.subTest(version=version, certificate=name):
                    self.check_identity(name, '::1', version, expected, bind='::1')


if __name__ == '__main__':
    unittest.main()
