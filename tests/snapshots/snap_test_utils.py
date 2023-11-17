# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
# Future
from __future__ import unicode_literals

# Third Party
from snapshottest import Snapshot

snapshots = Snapshot()

snapshots["test_sanitize_for_serialization_types 1"] = (
    {
        "kind": "Foo",
        "metadata": {"name": "test"},
        "spec": {
            "date": "2020-01-01T00:00:00",
            "list": ["listitem"],
            "openapiType": {
                "apiVersion": "v1",
                "kind": "Test",
                "metadata": {"name": "test"},
                "spec": {"container": []},
                "status": {"reconciledVersion": 1},
            },
            "resourceNode": {"metadata": {"name": "test"}},
            "should_be_empty": {},
            "tuple": ("tupleitem",),
        },
    },
)
