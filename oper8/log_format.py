"""
Custom logging formats that contain more detailed oper8 logs
"""

# First Party
from alog import AlogJsonFormatter
import alog

log = alog.use_channel("CTRLR")


class Oper8JsonFormatter(AlogJsonFormatter):
    """Custom Log Format that extends AlogJsonFormatter to add multiple
    oper8 specific fields to the json. This includes things like identifiers
    of the resource being reconciled, reconciliationId, and thread information
    """

    _FIELDS_TO_PRINT = AlogJsonFormatter._FIELDS_TO_PRINT + [
        "process",
        "thread",
        "threadName",
        "kind",
        "apiVersion",
        "resourceVersion",
        "resourceName",
        "reconciliationId",
    ]

    def __init__(self, manifest=None, reconciliation_id=None):
        super().__init__()
        self.manifest = manifest
        self.reconciliation_id = reconciliation_id

    def format(self, record):
        if self.reconciliation_id:
            record.reconciliationId = self.reconciliation_id

        if resource := getattr(record, "resource", self.manifest):
            record.kind = resource.get("kind")
            record.apiVersion = resource.get("apiVersion")

            metadata = resource.get("metadata", {})
            record.resourceVersion = metadata.get("resourceVersion")
            record.resourceName = metadata.get("name")

        return super().format(record)
