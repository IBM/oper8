"""
Thread class that will dump a heartbeat to a file periodically
"""

# Standard
from datetime import datetime, timedelta

# Local
from ....exceptions import assert_config
from ..utils import parse_time_delta
from .timer import TimerThread


class HeartbeatThread(TimerThread):
    """The HeartbeatThread acts as a pulse for the PythonWatchManager.

    This thread will periodically dump the value of "now" to a file which can be
    read by an observer such as a liveness/readiness probe to ensure that the
    manager is functioning well.
    """

    # This format is designed to be read using `date -d $(cat heartbeat.txt)`
    # using the GNU date utility
    # CITE: https://www.gnu.org/software/coreutils/manual/html_node/Examples-of-date.html
    _DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(self, heartbeat_file: str, heartbeat_period: str):
        """Initialize with the file location for the heartbeat output

        Args:
            heartbeat_file: str
                The fully-qualified path to the heartbeat file
            heartbeat_period: str
                Time delta string representing period delay between beats.
                NOTE: The GNU `date` utility cannot parse sub-seconds easily, so
                    the expected configuration for this is to be >= 1s
        """
        self._heartbeat_file = heartbeat_file
        self._offset = parse_time_delta(heartbeat_period)
        assert_config(
            self._offset >= timedelta(seconds=1),
            "heartbeat_period must be >= 1s",
        )
        super().__init__(name="heartbeat_thread")
        self.put_event(datetime.now(), self._run_heartbeat)

    def _run_heartbeat(self):
        """Run the heartbeat dump to the heartbeat file and put the next beat"""
        now = datetime.now()
        with open(self._heartbeat_file, "w") as handle:
            handle.write(now.strftime(self._DATE_FORMAT))
        self.put_event(now + self._offset, self._run_heartbeat)
