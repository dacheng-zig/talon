#include <openssl/ssl.h>
#include <openssl/err.h>
void talon_ssl_no_renegotiation(SSL_CTX *context);
void talon_ssl_servername_callback(SSL_CTX *context,
    int (*callback)(SSL *, int *, void *), void *arg);
