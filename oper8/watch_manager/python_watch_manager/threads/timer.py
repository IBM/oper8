"""
The TimerThread is a helper class used to run schedule events
"""

# Standard
from datetime import datetime
from heapq import heappop, heappush
from typing import Any, Callable, Dict, List, Optional
import queue
import threading

# First Party
import alog

# Local
from ..utils import MIN_SLEEP_TIME, Singleton, TimerEvent
from .base import ThreadBase

log = alog.use_channel("TMRTHRD")


class TimerThread(ThreadBase, metaclass=Singleton):
    """The TimerThread class is a helper class to run scheduled actions. This is very similar
    to threading.Timer stdlib class except that it uses one shared thread for all events
    instead of a thread per event."""

    def __init__(self, name: Optional[str] = None):
        """Initialize a priorityqueue like object and a synchronization object"""
        super().__init__(name=name or "timer_thread", daemon=True)

        # Use a heap queue instead of a queue.PriorityQueue as we're already handling
        # synchronization with the notify condition
        # https://docs.python.org/3/library/heapq.html?highlight=heap#priority-queue-implementation-notes
        self.timer_heap = []
        self.notify_condition = threading.Condition()

    def run(self):
        """The TimerThread's control loop sleeps until the next schedule
        event and executes all pending actions."""
        if not self.check_preconditions():
            return

        while True:
            # Wait until the next event or a new event is pushed
            with self.notify_condition:
                time_to_sleep = self._get_time_to_sleep()
                if time_to_sleep:
                    log.debug2(
                        "Timer waiting %ss until next scheduled event", time_to_sleep
                    )
                else:
                    log.debug2("Timer waiting until event queued")
                self.notify_condition.wait(timeout=time_to_sleep)

            if not self.check_preconditions():
                return

            # Get all the events to be executed
            event_list = self._get_all_current_events()
            for event in event_list:
                log.debug("Timer executing action for event: %s", event)
                event.action(*event.args, **event.kwargs)

    ## Class Interface ###################################################

    def stop_thread(self):
        """Override stop_thread to wake the control loop"""
        super().stop_thread()
        # Notify timer thread of shutdown
        log.debug2("Acquiring notify condition for shutdown")
        with self.notify_condition:
            log.debug("Notifying TimerThread of shutdown")
            self.notify_condition.notify_all()

    ## Public Interface ###################################################

    def put_event(
        self, time: datetime, action: Callable, *args: Any, **kwargs: Dict
    ) -> Optional[TimerEvent]:
        """Push an event to the timer

        Args:
            time: datetime
                The datetime to execute the event at
            action: Callable
                The action to execute
            *args: Any
                Args to pass to the action
            **kwargs: Dict
                Kwargs to pass to the action

        Returns:
            event: Optional[TimerEvent]
                TimerEvent describing the event and can be cancelled
        """
        # Don't allow pushing to a stopped thread
        if self.should_stop():
            return None

        # Create a timer event and push it to the heap
        event = TimerEvent(time=time, action=action, args=args, kwargs=kwargs)
        with self.notify_condition:
            heappush(self.timer_heap, event)
            self.notify_condition.notify_all()
        return event

    ## Time Functions  ###################################################

    def _get_time_to_sleep(self) -> Optional[int]:
        """Calculate the time to sleep based on the current queue

        Returns:
            time_to_wait: Optional[int]
               The time to wait if there's an object in the queue"""
        with self.notify_condition:
            obj = self._peak_next_event()
            if obj:
                time_to_sleep = (obj.time - datetime.now()).total_seconds()
                if time_to_sleep < MIN_SLEEP_TIME:
                    return MIN_SLEEP_TIME
                return time_to_sleep

            return None

    ## Queue Functions  ###################################################

    def _get_all_current_events(self) -> List[TimerEvent]:
        """Get all the current events that should execute

        Returns:
            current_events: List[TimerEvent]
                List of timer events to execute
        """
        event_list = []
        # With lock preview the next object
        with self.notify_condition:
            while len(self.timer_heap) != 0:
                obj_preview = self._peak_next_event()
                # If object exists and should've already executed then remove object from queue
                # and add it to return list
                if obj_preview and obj_preview.time < datetime.now():
                    try:
                        obj = heappop(self.timer_heap)
                        if obj.stale:
                            log.debug2("Skipping timer event %s", obj)
                            continue
                        event_list.append(obj)
                    except queue.Empty:
                        break
                else:
                    break
        return event_list

    def _peak_next_event(self) -> Optional[TimerEvent]:
        """Get the next timer event without removing it from the queue

        Returns:
            next_event: TimerEvent
                The next timer event if one exists
        """
        with self.notify_condition:
            if self.timer_heap:
                return self.timer_heap[0]
            return None
