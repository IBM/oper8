"""Log handler helper class"""

# Standard
from logging import Formatter, LogRecord
from logging.handlers import QueueHandler
import copy

# First Party
import alog

# Local
from ....managed_object import ManagedObject

log = alog.use_channel("LOG-HANDLER")

# Forward declaration of a queue for any type
QUEUE_TYPE = "Queue[Any]"


class LogQueueHandler(QueueHandler):
    """
    Log Handler class to collect messages from a child processes and pass
    them to the root process via a multiprocess queue
    """

    def __init__(self, queue: QUEUE_TYPE, manifest: ManagedObject = None):
        """Initialize the queue handler and instance variables

        Args:
            queue: "Queue[Any]"
                The queue to pass messages to
            manifest: ManagedObject
                The manifest of the current process. This is only used if it can't find
                the resource on the current formatter
        """
        super().__init__(queue)
        self.manifest = manifest

    def prepare(self, record: LogRecord) -> LogRecord:
        """Prep a record for pickling before sending it to the queue

        Args:
            record: LogRecord
                The record to be prepared

        Returns:
            prepared_record: LogRecord
                The prepared record ready to be pickled
        """

        # Duplicate record to preserve other handlers
        record = copy.copy(record)

        # get the currently used formatter
        formatter = self.formatter if self.formatter else Formatter()

        # Exceptions can't always be pickled so manually process
        # the record but remove the exc_info This retains the
        # the processed exc_txt but allows the parent process to reformat
        # the message
        if record.exc_info:
            record.exc_text = formatter.formatException(record.exc_info)
            record.exc_info = None

        # In case there are exceptions/unpicklable objects in the logging
        # args then manually compute the message. After computing clear the
        # message&args values to allow the parent process to reformat the
        # record
        record.msg = record.getMessage()
        record.args = []

        # Take the manifest from the current formatter and pass it back up
        resource = {}
        if hasattr(formatter, "manifest"):
            resource = formatter.manifest
        elif self.manifest:
            resource = self.manifest

        # Only copy required resource keys to the record
        resource_metadata = resource.get("metadata", {})
        record.resource = {
            "kind": resource.get("kind"),
            "apiVersion": resource.get("apiVersion"),
            "metadata": {
                "name": resource_metadata.get("name"),
                "namespace": resource_metadata.get("namespace"),
                "resourceVersion": resource_metadata.get("resourceVersion"),
            },
        }

        return record
