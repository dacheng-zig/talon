#include "tls_openssl.h"

/* Compile OpenSSL's uint64 option macro as C instead of translating shifts. */
void talon_ssl_no_renegotiation(SSL_CTX *context) {
    SSL_CTX_set_options(context, SSL_OP_NO_RENEGOTIATION);
}

/* These callback setters are macros using OpenSSL's generic control API. */
void talon_ssl_servername_callback(SSL_CTX *context,
    int (*callback)(SSL *, int *, void *), void *arg) {
    SSL_CTX_set_tlsext_servername_callback(context, callback);
    SSL_CTX_set_tlsext_servername_arg(context, arg);
}
