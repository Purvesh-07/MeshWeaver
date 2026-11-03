import time

from meshweaver.protocol import make_msg, verify
from meshweaver.security import ReplayGuard

KEY = b'secret'


def test_tampered_message_rejected():
    msg = make_msg('PING', 'abc', {'x': 1}, KEY)
    msg['body']['x'] = 2
    assert not verify(msg, KEY)


def test_wrong_key_rejected():
    assert not verify(make_msg('PING', 'abc', {}, KEY), b'other-key')


def test_stale_message_rejected():
    msg = make_msg('PING', 'abc', {}, KEY)
    time.sleep(0.05)
    assert not verify(msg, KEY, max_age=0.01)


def test_replay_detected():
    guard = ReplayGuard()
    assert not guard.seen('id-1')
    assert guard.seen('id-1')
