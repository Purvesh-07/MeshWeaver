import hashlib
import hmac
import json
import os
import time
import uuid


def new_node_id():
    # random 160-bit integer, like a SHA-1 hash
    return int.from_bytes(hashlib.sha1(os.urandom(32)).digest(), 'big')


def to_hex(n):
    return f'{n:040x}'


def canonical(d):
    # the same dict must always give the same bytes, otherwise signatures break
    return json.dumps(d, sort_keys=True, separators=(',', ':')).encode()


def make_msg(type_, sender, body, key, reply_to=None):
    msg = {
        'v': 1,
        'type': type_,
        'msg_id': uuid.uuid4().hex,
        'reply_to': reply_to,
        'sender': sender,
        'ts': time.time(),
        'body': body,
    }
    msg['sig'] = hmac.new(key, canonical(msg), hashlib.sha256).hexdigest()
    return msg


def verify(msg, key, max_age=30.0):
    # True only if the signature is right AND the message is fresh
    try:
        body = {k: v for k, v in msg.items() if k != 'sig'}
        expected = hmac.new(key, canonical(body), hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, msg['sig']) and abs(time.time() - body['ts']) <= max_age
    except (KeyError, TypeError, AttributeError):
        return False


def encode(msg):
    return json.dumps(msg).encode()


def decode(data):
    try:
        msg = json.loads(data)
    except (ValueError, UnicodeDecodeError):
        return None
    return msg if isinstance(msg, dict) else None
