"""
Tests for the HeartbeatThread
"""
# Standard
from datetime import datetime, timedelta
import tempfile
import time

# Third Party
import pytest

# Local
from oper8.exceptions import ConfigError
from oper8.watch_manager.python_watch_manager.threads import HeartbeatThread


class NonSingletonHeartbeatThread(HeartbeatThread):
    _disable_singleton = True


def test_simple_heartbeat():
    with tempfile.NamedTemporaryFile() as heartbeat_file:
        hb = NonSingletonHeartbeatThread(heartbeat_file.name, "1s")
        hb.start()
        time.sleep(1)
        hb.stop_thread()
        hb.join()
        with open(heartbeat_file.name) as handle:
            hb_str = handle.read()
        parsed = datetime.strptime(HeartbeatThread._DATE_FORMAT, hb_str)
        assert parsed > (datetime.now() - timedelta(seconds=5))
