# Zig TLS Client compatibility source

`tls_client.zig` is copied from the installed Zig 0.17.1 distribution's
`lib/std/crypto/tls/Client.zig`. Upstream project: https://github.com/ziglang/zig.
The original file SHA-256 is
`c6fbf80138bcf5cf5e46090294e94159815e2e175df56256679ddec87cf64d2c`.
Copyright Zig contributors; MIT license is preserved in `LICENSE`.

Local changes are limited to:

- Import `std` by module name and the Talon IP identity helper.
- Add a copied `host.ip` reference identity, with no SNI.
- Check the first certificate's IP SAN using the helper at the existing host
  verification point, before certificate-chain and handshake authentication.

Record encryption, key derivation, CA verification, handshake signatures,
Finished authentication and application I/O remain the distribution source.
Compare the complete source on each compiler upgrade; don't silently refresh
this file or assume the private std TLS structure is version independent.
Remove the compatibility source when std exposes an equivalent identity hook.

Contract and verification: [TLS IP identity](../../../../docs/design/tls.md).
