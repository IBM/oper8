"""
Thread class that will dump a heartbeat to a file periodically
"""

# Standard
from datetime import datetime
import threading

# First Party
import alog

# Local
from ..utils import parse_time_delta
from .timer import TimerThread

log = alog.use_channel("HBEAT")


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
        self._beat_lock = threading.Lock()
        self._beat_event = threading.Event()
        super().__init__(name="heartbeat_thread")

    def run(self):
        self._run_heartbeat()
        return super().run()

    def wait_for_beat(self):
        """Wait for the next beat"""
        # Make sure the beat lock is not held before starting wait. This
        # prevents beats that are immediately ready
        with self._beat_lock:
            pass

        # Wait for the next beat
        self._beat_event.wait()

    def _run_heartbeat(self):
        """Run the heartbeat dump to the heartbeat file and put the next beat"""
        now = datetime.now()
        log.debug3("Heartbeat %s", now)

        # Save the beat to disk
        try:
            with open(self._heartbeat_file, "w", encoding="utf-8") as handle:
                handle.write(now.strftime(self._DATE_FORMAT))
                handle.flush()
        except Exception as err:
            log.warning("Failed to write heartbeat file: %s", err, exc_info=True)

        # Unblock and reset the wait condition
        with self._beat_lock:
            self._beat_event.set()
            self._beat_event.clear()

        # Put the next beat if not stopped
        if not self.should_stop():
            self.put_event(now + self._offset, self._run_heartbeat)
