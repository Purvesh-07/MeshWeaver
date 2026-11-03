import ssl
import time
from collections import OrderedDict


class ReplayGuard:
    # remembers recent message IDs; a repeated ID means a replay
    def __init__(self, ttl=60.0, max_items=10000):
        self.ttl, self.max_items = ttl, max_items
        self.ids = OrderedDict()

    def seen(self, msg_id):
        now = time.monotonic()
        while self.ids and (len(self.ids) > self.max_items
                            or now - next(iter(self.ids.values())) > self.ttl):
            self.ids.popitem(last=False)
        if msg_id in self.ids:
            return True
        self.ids[msg_id] = now
        return False


class RateLimiter:
    # token bucket per source IP: about `rate` messages per second, bursts up to `burst`
    def __init__(self, rate=100.0, burst=200.0):
        self.rate, self.burst = rate, burst
        self.buckets = {}

    def allow(self, ip):
        now = time.monotonic()
        tokens, last = self.buckets.get(ip, (self.burst, now))
        tokens = min(self.burst, tokens + (now - last) * self.rate)
        if tokens < 1:
            self.buckets[ip] = (tokens, now)
            return False
        self.buckets[ip] = (tokens - 1, now)
        return True


def make_contexts(tls):
    # returns (server_context, client_context) for mutual TLS
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.load_cert_chain(tls['cert'], tls['key'])
    server.load_verify_locations(tls['ca'])
    server.verify_mode = ssl.CERT_REQUIRED         # clients MUST present a certificate

    client = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    client.load_cert_chain(tls['cert'], tls['key'])
    client.load_verify_locations(tls['ca'])
    client.check_hostname = False                  # peers are identified by certificate, not by IP
    client.verify_mode = ssl.CERT_REQUIRED

    for ctx in (server, client):
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT   # allows simple self-made certificates
    return server, client
