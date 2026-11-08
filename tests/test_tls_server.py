"""Optional real TLS server acceptance; requires zig-out/bin/tls_server_probe."""
from pathlib import Path
import re
import hashlib
import socket
import ssl
import struct
import subprocess
import tempfile
import threading
import time
import unittest


ROOT = Path(__file__).resolve().parent.parent
PROBE = ROOT/'zig-out/bin/tls_server_probe'


def exact(stream, count):
    data = bytearray()
    while len(data) < count:
        part = stream.recv(count-len(data))
        if not part:
            raise ValueError('unexpected TLS EOF')
        data.extend(part)
    return bytes(data)


def send_frame(stream, opcode, payload):
    mask = b'\x12\x34\x56\x78'
    length = len(payload)
    head = bytes([128|opcode,128|length]) if length < 126 else bytes([128|opcode,128|126])+struct.pack('!H',length)
    stream.sendall(head+mask+bytes(v^mask[i%4] for i,v in enumerate(payload)))


@unittest.skipUnless(PROBE.exists(), 'optional OpenSSL server probe not built')
class ServerTlsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.directory = Path(cls.temp.name)
        cls.cert,cls.key = cls.directory/'cert.pem',cls.directory/'key.pem'
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
                        '-subj','/CN=localhost','-addext','subjectAltName=DNS:localhost,IP:127.0.0.1',
                        '-keyout',str(cls.key),'-out',str(cls.cert)],check=True,capture_output=True)
        cls.certificates = {'default': cls.cert}
        for label in ['alpha', 'beta']:
            cert, key = Path(str(cls.cert)+f'.{label}.pem'), Path(str(cls.key)+f'.{label}.pem')
            hostname = f'{label}.localhost'
            subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
                            '-subj', f'/CN={hostname}', '-addext', f'subjectAltName=DNS:{hostname}',
                            '-keyout', str(key), '-out', str(cert)], check=True, capture_output=True)
            cls.certificates[label] = cert
        cls.trust = cls.directory/'trust.pem'
        cls.trust.write_text(''.join(cert.read_text() for cert in cls.certificates.values()))

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def start(self, duration=1500, mode=None):
        self.lines = []
        self.facts = {}
        command=[str(PROBE),str(self.cert),str(self.key),str(duration)]
        if mode: command.append(mode)
        self.process = subprocess.Popen(command,
                                        stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
        def read():
            for line in self.process.stdout:
                self.lines.append(line.rstrip())
        self.reader = threading.Thread(target=read)
        self.reader.start()
        self.addCleanup(self.finish)
        deadline = time.monotonic()+10
        while time.monotonic() < deadline:
            for line in self.lines:
                match = re.search(r'tls_server_port=(\d+)',line)
                if match: return int(match[1])
            if self.process.poll() is not None: break
            time.sleep(.01)
        self.fail('TLS server did not become ready: '+str(self.lines))

    def finish(self):
        try:
            code = self.process.wait(timeout=12)
        except subprocess.TimeoutExpired:
            self.process.kill()
            code = self.process.wait()
        self.reader.join(timeout=3)
        self.process.stdout.close()
        self.assertFalse(self.reader.is_alive())
        self.assertEqual(code,0,str(self.lines))
        final = next(line for line in self.lines if 'tls_server_done=' in line)
        values = dict(re.findall(r'(\w+)=(\d+)',final))
        self.assertEqual(values['accepted'],values['finished'])
        self.assertEqual(values['active'],'0')
        self.assertTrue(any('tls_server_budget=0' in line for line in self.lines))
        if 'silent_handshake' in self.id():
            self.assertGreaterEqual(int(values['handshake_timeouts']),1)
        if 'stalled_established' in self.id() or 'forced_drain' in self.id():
            self.assertGreaterEqual(int(values['forced_drains']),1)
        if 'expired_write_deadline' in self.id():
            self.assertEqual(int(values['connection_errors']),1)
        if 'alpn_expected_native' in self.facts:
            selected = [re.search(r'tls_alpn_selected=(\S+)', line)[1] for line in self.lines if 'tls_alpn_selected=' in line]
            self.assertEqual(selected, self.facts['alpn_expected_native'])
            self.assertEqual(values['connection_errors'], '0')
            self.assertEqual(values['forced_drains'], '0')
            self.assertEqual(values['transport_failures'], '1' if 'alpn_rejected' in self.facts else '0')

    def connect(self,port,version=None,hostname='localhost',strict=False):
        context = ssl.create_default_context(cafile=str(self.cert))
        if version: context.minimum_version=context.maximum_version=version
        raw = socket.create_connection(('127.0.0.1',port),timeout=3)
        try: return context.wrap_socket(raw,server_hostname=hostname,suppress_ragged_eofs=not strict)
        except BaseException:
            raw.close()
            raise

    def test_tls12_and_tls13_http_and_reuse(self):
        port = self.start(2000)
        for version in [ssl.TLSVersion.TLSv1_2,ssl.TLSVersion.TLSv1_3]:
            with self.connect(port,version) as stream:
                for _ in range(2):
                    stream.sendall(b'GET / HTTP/1.1\r\nHost: localhost\r\n\r\n')
                    head=bytearray()
                    while not head.endswith(b'\r\n\r\n'): head.extend(exact(stream,1))
                    self.assertTrue(head.startswith(b'HTTP/1.1 200'))
                    length=int(re.search(rb'content-length:\s*(\d+)',head,re.I)[1])
                    self.assertEqual(exact(stream,length),b'secure talon\n')

    def alpn_connect(self, port, version, protocols):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.load_verify_locations(cafile=str(self.cert))
        context.minimum_version = context.maximum_version = version
        if protocols: context.set_alpn_protocols(protocols)
        raw = socket.create_connection(('127.0.0.1', port), timeout=3)
        try: return context.wrap_socket(raw, server_hostname='localhost')
        except BaseException:
            raw.close()
            raise

    def alpn_response(self, port, version, offered, expected):
        with self.alpn_connect(port, version, offered) as stream:
            self.assertEqual(stream.selected_alpn_protocol(), expected)
            stream.sendall(b'GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n')
            data = bytearray()
            while part := stream.recv(8192): data.extend(part)
            self.assertTrue(data.startswith(b'HTTP/1.1 200 OK\r\n'))
            self.assertEqual(bytes(data).split(b'\r\n\r\n', 1)[1], b'secure talon\n')

    def test_alpn_tls12(self):
        self.check_alpn(ssl.TLSVersion.TLSv1_2)

    def test_alpn_tls13(self):
        self.check_alpn(ssl.TLSVersion.TLSv1_3)

    def check_alpn(self, version):
        port = self.start(1500, 'alpn')
        self.alpn_response(port, version, ['http/1.1'], 'http/1.1')
        self.alpn_response(port, version, ['h2', 'http/1.1'], 'http/1.1')
        self.alpn_response(port, version, [], None)
        with self.assertRaises(ssl.SSLError) as rejected:
            self.alpn_connect(port, version, ['h2'])
        self.assertEqual(rejected.exception.reason, 'TLSV1_ALERT_NO_APPLICATION_PROTOCOL')
        self.facts['alpn_rejected'] = rejected.exception.reason
        self.alpn_response(port, version, ['http/1.1'], 'http/1.1')
        self.facts['alpn_expected_native'] = ['http/1.1', 'http/1.1', 'none', 'http/1.1']

    def test_alpn_disabled_preserves_legacy_handshake(self):
        port = self.start(1500, 'alpn-disabled')
        for version in [ssl.TLSVersion.TLSv1_2, ssl.TLSVersion.TLSv1_3]:
            self.alpn_response(port, version, ['h2'], None)
        self.facts['alpn_expected_native'] = ['none', 'none']

    def sni_context(self, version, check_hostname=True, protocols=('http/1.1',)):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.load_verify_locations(cafile=str(self.trust))
        context.check_hostname = check_hostname
        context.minimum_version = context.maximum_version = version
        context.set_alpn_protocols(list(protocols))
        return context

    def sni_connect(self, port, context, hostname, session=None):
        raw = socket.create_connection(('127.0.0.1', port), timeout=3)
        try: return context.wrap_socket(raw, server_hostname=hostname, session=session, suppress_ragged_eofs=False)
        except BaseException:
            raw.close()
            raise

    def sni_response(self, port, context, hostname, identity, reused=False, session=None):
        with self.sni_connect(port, context, hostname, session) as stream:
            digest = hashlib.sha256(stream.getpeercert(binary_form=True)).hexdigest()
            expected = hashlib.sha256(ssl.PEM_cert_to_DER_cert(self.certificates[identity].read_text())).hexdigest()
            self.assertEqual(digest, expected)
            self.assertEqual(stream.session_reused, reused)
            self.assertEqual(stream.selected_alpn_protocol(), 'http/1.1')
            stream.sendall(b'GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n')
            data = bytearray()
            while part := stream.recv(8192): data.extend(part)
            self.assertTrue(data.startswith(b'HTTP/1.1 200 OK\r\n'))
            head, body = bytes(data).split(b'\r\n\r\n', 1)
            self.assertEqual(body, b'secure talon\n')
            self.assertEqual(int(re.search(rb'content-length:\s*(\d+)', head, re.I)[1]), len(body))
            current = stream.session  # TLS 1.3 tickets arrive after handshake.
            return current

    def check_sni_matrix(self, version):
        port = self.start(2000, 'sni')
        for hostname, identity in [('alpha.localhost', 'alpha'), ('beta.localhost', 'beta'),
                                   ('ALPHA.LOCALHOST', 'alpha'), (None, 'default'), ('unknown.localhost', 'default')]:
            self.sni_response(port, self.sni_context(version, hostname not in (None, 'unknown.localhost')),
                              hostname, identity)
        with self.assertRaises(ssl.SSLCertVerificationError):
            self.sni_connect(port, self.sni_context(version), 'unknown.localhost')
        with self.assertRaises(ssl.SSLError) as rejected:
            self.sni_connect(port, self.sni_context(version, protocols=['h2']), 'alpha.localhost')
        self.assertEqual(rejected.exception.reason, 'TLSV1_ALERT_NO_APPLICATION_PROTOCOL')
        self.facts['sni_alpn_rejected'] = rejected.exception.reason

    def test_sni_matrix_tls12(self):
        self.check_sni_matrix(ssl.TLSVersion.TLSv1_2)

    def test_sni_matrix_tls13(self):
        self.check_sni_matrix(ssl.TLSVersion.TLSv1_3)

    def check_sni_resumption(self, version):
        port = self.start(2500, 'sni')
        context = self.sni_context(version)
        alpha = self.sni_response(port, context, 'alpha.localhost', 'alpha')
        self.assertTrue(alpha.has_ticket)
        self.sni_response(port, context, 'alpha.localhost', 'alpha', True, alpha)
        beta = self.sni_response(port, context, 'beta.localhost', 'beta', False, alpha)
        self.assertTrue(beta.has_ticket)
        self.sni_response(port, context, 'beta.localhost', 'beta', True, beta)
        self.sni_response(port, context, 'alpha.localhost', 'alpha', False, beta)
        default = self.sni_response(port, context, 'localhost', 'default', False, alpha)
        self.assertTrue(default.has_ticket)
        self.sni_response(port, context, 'localhost', 'default', True, default)
        self.sni_response(port, context, 'alpha.localhost', 'alpha', False, default)
        context.check_hostname = False  # Still verify the default certificate chain.
        self.sni_response(port, context, None, 'default', True, default)
        self.sni_response(port, context, 'unknown.localhost', 'default', True, default)

    def test_sni_resumption_tls12(self):
        self.check_sni_resumption(ssl.TLSVersion.TLSv1_2)

    def test_sni_resumption_tls13(self):
        self.check_sni_resumption(ssl.TLSVersion.TLSv1_3)

    def test_sni_malformed_extension_is_rejected(self):
        port = self.start(2000, 'sni')
        incoming, outgoing = ssl.MemoryBIO(), ssl.MemoryBIO()
        client = self.sni_context(ssl.TLSVersion.TLSv1_2).wrap_bio(incoming, outgoing, server_hostname='alpha.localhost')
        with self.assertRaises(ssl.SSLWantReadError): client.do_handshake()
        hello = outgoing.read()
        # Locate extension 0 by walking the ClientHello vectors, not searching
        # for a hostname byte pattern in random/cipher material.
        self.assertEqual(hello[:1], b'\x16')
        self.assertEqual(int.from_bytes(hello[3:5], 'big'), len(hello)-5)
        cursor = 9+2+32  # TLS record + handshake header + version + random.
        cursor += 1+hello[cursor]
        cursor += 2+int.from_bytes(hello[cursor:cursor+2], 'big')
        cursor += 1+hello[cursor]
        end = cursor+2+int.from_bytes(hello[cursor:cursor+2], 'big')
        cursor += 2
        self.assertEqual(end, len(hello))
        while cursor < end:
            kind, length = struct.unpack('!HH', hello[cursor:cursor+4])
            if kind == 0: break
            cursor += 4+length
        self.assertLess(cursor, end)
        offset = cursor+4
        variants = [('list_length', offset+1, 0), ('name_length', offset+4, 0),
                    ('unsupported_type', offset+2, 1), ('embedded_nul', offset+5, 0)]
        for label, index, value in variants:
            altered = bytearray(hello)
            self.assertNotEqual(altered[index], value)
            altered[index] = value
            with socket.create_connection(('127.0.0.1', port), timeout=3) as stream:
                stream.sendall(altered)
                head = exact(stream, 5)
                body = exact(stream, int.from_bytes(head[3:5], 'big'))
                self.assertEqual(head[0], 21)
                self.assertEqual(body, b'\x02\x32')  # fatal decode_error

    def test_sni_configuration_allocation_and_identity_rollback(self):
        command = [str(PROBE), str(self.cert), str(self.key), '1000', 'sni-check']
        result = subprocess.run(command, capture_output=True, text=True, timeout=10)
        raw = (result.stdout+result.stderr).splitlines()
        self.assertEqual(result.returncode, 0, raw)
        self.assertTrue(any('tls_sni_configuration_checked=1' in line for line in raw))
        self.assertFalse(any('tls_server_port=' in line for line in raw))

    def test_invalid_identity_is_rejected_before_accept(self):
        invalid=self.directory/'invalid-key.pem'
        invalid.write_text('not a private key\n')
        result=subprocess.run([str(PROBE),str(self.cert),str(invalid),'1000'],capture_output=True,text=True,timeout=5)
        self.assertNotEqual(result.returncode,0)
        self.assertNotIn('tls_server_port=',result.stdout+result.stderr)

    def test_wrong_hostname_plaintext_and_silent_handshake(self):
        port=self.start(2000)
        with self.assertRaises(ssl.SSLCertVerificationError): self.connect(port,hostname='wrong.invalid')
        with socket.create_connection(('127.0.0.1',port),timeout=2) as raw:
            raw.sendall(b'GET / HTTP/1.1\r\nHost: x\r\n\r\n')
            try: data=raw.recv(128)
            except ConnectionResetError: data=b''
            self.assertFalse(data.startswith(b'HTTP/'))
        with socket.create_connection(('127.0.0.1',port),timeout=2) as raw:
            self.assertEqual(raw.recv(1),b'')

    def test_websocket_ping_data_and_close_over_tls(self):
        port=self.start(2000)
        with self.connect(port) as stream:
            stream.sendall(b'GET /ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n')
            head=bytearray()
            while not head.endswith(b'\r\n\r\n'): head.extend(exact(stream,1))
            self.assertTrue(head.startswith(b'HTTP/1.1 101'))
            send_frame(stream,9,b'ping')
            self.assertEqual(exact(stream,6),b'\x8a\x04ping')
            send_frame(stream,2,b'x'*900)
            self.assertEqual(exact(stream,4),b'\x82\x7e\x03\x84')
            self.assertEqual(exact(stream,900),b'x'*900)
            send_frame(stream,8,b'\x03\xe8')
            self.assertEqual(exact(stream,4),b'\x88\x02\x03\xe8')

    def test_stalled_established_connection_is_drained(self):
        port=self.start()
        with self.connect(port) as stream:
            stream.sendall(b'GET /ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n')
            head=bytearray()
            while not head.endswith(b'\r\n\r\n'): head.extend(exact(stream,1))
            self.assertTrue(head.startswith(b'HTTP/1.1 101'))
            stream.sendall(b'\x82\xfe\x03\xe8\x12\x34\x56\x78\x6a')
            # Default teardown is raw close, not a complete TLS close handshake.
            try: data=stream.recv(1)
            except (ssl.SSLError,ConnectionResetError): data=b''
            self.assertEqual(data,b'')

    def test_two_senders_and_automatic_pong_over_tls(self):
        port=self.start(2500)
        with self.connect(port) as stream:
            stream.sendall(b'GET /duplex HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n')
            head=bytearray()
            while not head.endswith(b'\r\n\r\n'): head.extend(exact(stream,1))
            self.assertTrue(head.startswith(b'HTTP/1.1 101'))
            send_frame(stream,9,b'concurrent')
            counts={65:0,66:0}
            assembled=bytearray()
            pong=False
            while sum(counts.values()) < 6 or not pong:
                first,second=exact(stream,2)
                self.assertFalse(second&128)
                length=second&127
                if length==126: length=struct.unpack('!H',exact(stream,2))[0]
                elif length==127: length=struct.unpack('!Q',exact(stream,8))[0]
                self.assertLessEqual(length,4096)
                payload=exact(stream,length)
                opcode=first&15
                if opcode==10:
                    self.assertEqual(payload,b'concurrent')
                    self.assertFalse(pong)
                    pong=True
                    continue
                self.assertEqual(opcode,2 if not assembled else 0)
                assembled.extend(payload)
                if first&128:
                    self.assertEqual(len(assembled),65536)
                    sender,sequence=assembled[:2]
                    self.assertIn(sender,counts)
                    self.assertEqual(sequence,counts[sender])
                    self.assertEqual(assembled[2:],bytes([sender])*65534)
                    counts[sender]+=1
                    assembled.clear()
            self.assertEqual(counts,{65:3,66:3})
            send_frame(stream,1,b'done')
            self.assertEqual(exact(stream,4),b'\x88\x02\x03\xe8')

    def test_explicit_close_notify_is_clean_tls_eof(self):
        port=self.start(mode='notify')
        context=ssl.create_default_context(cafile=str(self.cert))
        with context.wrap_socket(socket.create_connection(('127.0.0.1',port),timeout=3),server_hostname='localhost',suppress_ragged_eofs=False) as stream:
            self.assertEqual(exact(stream,6),b'notify')
            self.assertEqual(stream.recv(1),b'')

    def test_partial_tls_record_survives_idle_timeout_tick(self):
        port=self.start(4000)
        context=ssl.create_default_context(cafile=str(self.cert))
        incoming,outgoing=ssl.MemoryBIO(),ssl.MemoryBIO()
        client=context.wrap_bio(incoming,outgoing,server_hostname='localhost')
        with socket.create_connection(('127.0.0.1',port),timeout=3) as raw:
            while True:
                try:
                    client.do_handshake()
                    raw.sendall(outgoing.read())
                    break
                except ssl.SSLWantReadError:
                    raw.sendall(outgoing.read())
                    incoming.write(raw.recv(65536))
            def response():
                result=bytearray()
                while not result.endswith(b'secure talon\n'):
                    try: result.extend(client.read(65536))
                    except ssl.SSLWantReadError:
                        pending=outgoing.read()
                        if pending: raw.sendall(pending)
                        wire=raw.recv(65536)
                        self.assertTrue(wire)
                        incoming.write(wire)
                self.assertTrue(result.startswith(b'HTTP/1.1 200'))
            request=b'GET / HTTP/1.1\r\nHost: localhost\r\n\r\n'
            client.write(request)
            raw.sendall(outgoing.read())
            response()
            client.write(request)
            wire=outgoing.read()
            self.assertGreater(len(wire),5)
            raw.sendall(wire[:5])
            time.sleep(1.15) # Cross the engine's one-second idle read tick.
            raw.sendall(wire[5:])
            response()

    def test_normal_http_finish_sends_alert_without_waiting_for_peer(self):
        port=self.start(2000,mode='finish')
        for version in [ssl.TLSVersion.TLSv1_2,ssl.TLSVersion.TLSv1_3]:
            with self.connect(port,version,strict=True) as stream:
                started=time.monotonic()
                stream.sendall(b'GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n')
                response=bytearray()
                while part:=stream.recv(65536): response.extend(part)
                self.assertTrue(response.startswith(b'HTTP/1.1 200'))
                self.assertTrue(response.endswith(b'secure talon\n'))
                self.assertLess(time.monotonic()-started,1)

    def test_peer_initiated_tls_unwrap_completes(self):
        port=self.start(mode='finish')
        with self.connect(port,strict=True) as stream:
            stream.sendall(b'GET / HTTP/1.1\r\nHost: localhost\r\n\r\n')
            head=bytearray()
            while not head.endswith(b'\r\n\r\n'): head.extend(exact(stream,1))
            self.assertTrue(head.startswith(b'HTTP/1.1 200'))
            length=int(re.search(rb'content-length:\s*(\d+)',head,re.I)[1])
            self.assertEqual(exact(stream,length),b'secure talon\n')
            raw=stream.unwrap()
            raw.close()

    def test_normal_websocket_close_has_clean_tls_eof(self):
        port=self.start(mode='finish')
        with self.connect(port,strict=True) as stream:
            stream.sendall(b'GET /ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n')
            head=bytearray()
            while not head.endswith(b'\r\n\r\n'): head.extend(exact(stream,1))
            self.assertTrue(head.startswith(b'HTTP/1.1 101'))
            send_frame(stream,8,b'\x03\xe8')
            self.assertEqual(exact(stream,4),b'\x88\x02\x03\xe8')
            self.assertEqual(stream.recv(1),b'')

    def test_expired_write_deadline_skips_normal_alert(self):
        port=self.start(mode='expired')
        with self.connect(port,strict=True) as stream:
            self.assertEqual(exact(stream,7),b'expired')
            with self.assertRaises((ssl.SSLEOFError,ConnectionResetError)): stream.recv(1)

    def test_forced_drain_skips_normal_alert(self):
        port=self.start(mode='finish')
        with self.connect(port,strict=True) as stream:
            stream.sendall(b'GET /ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n')
            head=bytearray()
            while not head.endswith(b'\r\n\r\n'): head.extend(exact(stream,1))
            self.assertTrue(head.startswith(b'HTTP/1.1 101'))
            stream.sendall(b'\x82\xfe\x03\xe8\x12\x34\x56\x78\x6a')
            with self.assertRaises((ssl.SSLEOFError,ConnectionResetError)): stream.recv(1)


if __name__ == '__main__': unittest.main()
