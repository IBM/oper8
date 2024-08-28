"""
This module implements a mock of the kubernetes client library which can be used
to patch the api client in an ansible module.

We attempt to emulate the internals of the kubernetes api_client, but this is
based on code inspection of the current implementation and is certainly subject
to change!
"""

# Standard
from contextlib import contextmanager
from datetime import datetime, timedelta
from queue import Empty, Queue
from threading import Event, RLock
from typing import Optional
from unittest import mock
import base64
import json

# Third Party
from openshift.dynamic.apply import annotate as annotate_last_applied
import kubernetes

# First Party
import aconfig
import alog

# Local
from oper8.deploy_manager import KubeEventType

log = alog.use_channel("TEST")


class MockKubRestResponse(kubernetes.client.rest.RESTResponse):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def getheaders():
        return {}


class MockWatchStreamResponse:
    """Helper class used to stream resources from the MockKubeClient using a queue.
    When the streaming method is called the MockWatchStreamResponse registers a
    threading queue with the MockKubClient and yields all events"""

    def __init__(
        self,
        api_client: "MockKubClient",
        api_version: str,
        kind: str,
        namespace: Optional[str] = None,
        name: Optional[str] = None,
        timeout: Optional[int] = None,
    ):
        self.api_client = api_client
        self.watch_queue = Queue()
        self.timeout = timeout or 250

        self.kind = kind
        self.api_version = api_version
        self.namespace = namespace
        self.name = name

        # Shutdown flag
        self.shutdown = Event()

    def __del__(self):
        self.api_client._remove_watch_queue(self.watch_queue)

    def stream(self, *args, **kwargs):
        """Continuously yield events from the cluster until the shutdown or timeout"""

        # Get the current resource state
        current_resources = []
        if self.name:
            current_obj = self.api_client._get_object_state(
                method="GET",
                namespace=self.namespace,
                kind=self.kind,
                api_version=self.api_version,
                name=self.name,
            )
            if current_obj:
                current_resources.append(current_obj)
        else:
            response, code = self.api_client._list_object_state(
                method="GET",
                namespace=self.namespace,
                kind=self.kind,
                api_version=self.api_version,
            )
            current_resources = response.get("items")

        # yield back the resources
        for resource in current_resources:
            log.debug2("Yielding initial state event")
            yield self._make_watch_response(KubeEventType.ADDED, resource)

        log.debug2("Yielded initial state. Starting watch")
        # Create a watch queue and add it to the api_client
        self.api_client._add_watch_queue(self.watch_queue)

        # Configure the timeout and end times
        timeout_delta = timedelta(seconds=self.timeout)
        end_time = datetime.now() + timeout_delta
        while True:
            timeout = (end_time - datetime.now()).total_seconds() or 1
            try:
                event_type, resource = self.watch_queue.get(timeout=timeout)
            except Empty:
                return

            if self._check_end_conditions(end_time):
                return

            resource_metadata = resource.get("metadata", {})

            # Ensure the kind/apiversion/namespace match the requested
            if (
                resource.get("kind") == self.kind
                and resource.get("apiVersion") == self.api_version
                and resource_metadata.get("namespace") == self.namespace
            ):
                # If resourced then ensure the name matches
                if self.name and resource_metadata.get("name") != self.name:
                    continue

                log.debug2("Yielding watch event")
                yield self._make_watch_response(event_type, resource)

    def close(self):
        pass

    def release_conn(self):
        self.shutdown.set()

    def _check_end_conditions(self, end_time):
        log.debug3("Checking shutdown and endtime conditions")
        if self.shutdown.is_set():
            return True

        return end_time < datetime.now()

    def _make_watch_response(self, event, object):
        # Add new line to watch response
        response = json.dumps({"type": event.value, "object": object}) + "\n"
        return response


class MockKubClient(kubernetes.client.ApiClient):
    """Mocked version of kubernetes.client.ApiClient which swaps out the
    implementation of call_api() to use preconfigured responses
    """

    def __init__(self, cluster_state=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Save the current cluster state
        self.__cluster_state = cluster_state or {}
        log.debug3("Cluster State: %s", self.__cluster_state)

        # Setup listener variables
        self.__queue_lock = RLock()
        self.__watch_queues = set()

        # Save a list of kinds for each api group
        self._api_group_kinds = {}

        # Canned handlers
        self._handlers = {
            "/apis": {"GET": self.apis},
            "/version": {"GET": lambda *_, **__: self._make_response({})},
            "/api/v1": {"GET": self.api_v1},
        }
        for namespace, ns_entries in self.__cluster_state.items():
            for kind, kind_entries in ns_entries.items():
                for api_version, version_entries in kind_entries.items():
                    # Add handlers common to this kind
                    self._add_handlers_for_kind(namespace, kind, api_version)

                    # Add handlers for the individual pre-existing instances
                    for name, obj_state in version_entries.items():
                        # If this is a static object state, make sure the
                        # metadata aligns correctly
                        if isinstance(obj_state, dict):
                            self._add_resource_defaults(
                                obj_state=obj_state,
                                namespace=namespace,
                                kind=kind,
                                api_version=api_version,
                                name=name,
                            )

                        # Add the endpoints for this resource
                        self._add_handlers_for_resource(
                            namespace=namespace,
                            kind=kind,
                            api_version=api_version,
                            name=name,
                        )

        log.debug("Configured Handlers: %s", list(self._handlers.keys()))

    def call_api(
        self,
        resource_path,
        method,
        path_params=None,
        query_params=None,
        header_params=None,
        body=None,
        **kwargs,
    ):
        """Mocked out call function to return preconfigured responses

        NOTE: this is set up to work with how openshift.dynamic.DynamicClient
            calls self.client.call_api. It (currently) passes some as positional
            args and some as kwargs.
        """
        for key, value in query_params:
            if key == "watch" and value:
                method = "WATCH"
                break

        log.debug2("Mock [%s] request to [%s]", method, resource_path)
        log.debug4("Path Params: %s", path_params)
        log.debug4("Query Params: %s", query_params)
        log.debug4("Header Params: %s", header_params)
        log.debug4("Body: %s", body)

        # Find the right handler and execute it
        return self._get_handler(resource_path, method)(
            resource_path=resource_path,
            path_params=path_params,
            query_params=query_params,
            header_params=header_params,
            body=body,
            **kwargs,
        )

    ## Implementation Helpers ##################################################

    @staticmethod
    def _make_response(body, status_code=200):
        log.debug2("Making response with code: %d", status_code)
        log.debug4(body)
        resp = MockKubRestResponse(
            aconfig.Config(
                {
                    "status": status_code,
                    "reason": "MOCK",
                    "data": json.dumps(body).encode("utf8"),
                }
            )
        )
        if not 200 <= status_code <= 299:
            raise kubernetes.client.rest.ApiException(http_resp=resp)
        return resp

    def _get_handler(self, resource_path, method):
        # Look for a configured handler that matches the path exactly
        handler = self._handlers.get(resource_path, {}).get(method)

        # If no handler found, start looking for '*' handlers
        path_parts = resource_path.split("/")
        while handler is None and path_parts:
            star_path = "/".join(path_parts + ["*"])
            log.debug4("Looking for [%s]", star_path)
            handler = self._handlers.get(star_path, {}).get(method)
            path_parts.pop()

        # Return whatever we've found or not_found
        return handler or self.not_found

    @staticmethod
    def _get_kind_variants(kind):
        return [kind.lower(), f"{kind.lower()}s", kind]

    @staticmethod
    def _get_version_parts(api_version):
        parts = api_version.split("/", 1)
        if len(parts) == 2:
            group_name, version = parts
            api_endpoint = f"/apis/{group_name}/{version}"
        else:
            group_name = None
            version = api_version
            api_endpoint = f"/api/{api_version}"
        return group_name, version, api_endpoint

    def _add_handlers_for_kind(self, namespace, kind, api_version):
        # Set up the information needed for the apis and crds calls
        group_name, version, api_endpoint = self._get_version_parts(api_version)
        self._api_group_kinds.setdefault(group_name, {}).setdefault(version, []).append(
            kind
        )

        # Add a configured handler for this group type
        log.debug("Adding resource handler for: %s", api_endpoint)
        self._handlers[api_endpoint] = {
            "GET": lambda *_, **__: (self.current_state_crds(group_name, version))
        }

        # Add POST handlers for this type
        for kind_variant in self._get_kind_variants(kind):
            # Add different endpoints based on namespace
            if namespace:
                endpoint = f"{api_endpoint}/namespaces/{namespace}/{kind_variant}/*"
            else:
                endpoint = f"{api_endpoint}/{kind_variant}/*"

            log.debug2(
                "Adding POST & PUT & GET & WATCH & PATCH handler for (%s: %s)",
                kind,
                endpoint,
            )
            self._handlers[endpoint] = {
                "WATCH": lambda resource_path, body, *_, **__: (
                    self.current_state_watch(
                        resource_path, api_version, kind, resourced=False
                    )
                ),
                "PATCH": lambda resource_path, body, *_, **__: (
                    self.current_state_patch(resource_path, api_version, kind, body)
                ),
                "PUT": lambda resource_path, body, *_, **__: (
                    self.current_state_patch(resource_path, api_version, kind, body)
                ),
                "POST": lambda resource_path, body, *_, **__: (
                    self.current_state_post(resource_path, api_version, kind, body)
                ),
                "GET": lambda resource_path, *_, **__: (
                    self.current_state_list(resource_path, api_version, kind)
                ),
            }

    def _remove_handlers_for_resource(self, namespace, kind, api_version, name):
        # Get crucial API information out of the object
        _, __, api_endpoint = self._get_version_parts(api_version)
        for kind_variant in self._get_kind_variants(kind):
            if namespace:
                resource_api_endpoint = (
                    f"{api_endpoint}/namespaces/{namespace}/{kind_variant}/{name}"
                )
            else:
                resource_api_endpoint = f"{api_endpoint}/{kind_variant}/{name}"

            status_resource_api_endpoint = "/".join([resource_api_endpoint, "status"])
            del self._handlers[resource_api_endpoint]
            del self._handlers[status_resource_api_endpoint]

    def _add_handlers_for_resource(self, namespace, kind, api_version, name):
        # Get crucial API information out of the object
        _, __, api_endpoint = self._get_version_parts(api_version)

        # Add configured handlers for GET/PUT on this resource
        for kind_variant in self._get_kind_variants(kind):
            # The endpoint that will be used to hit this specific
            # resource
            if namespace:
                resource_api_endpoint = (
                    f"{api_endpoint}/namespaces/{namespace}/{kind_variant}/{name}"
                )
            else:
                resource_api_endpoint = f"{api_endpoint}/{kind_variant}/{name}"

            # Add the handlers
            log.debug("Adding GET handler for: %s", resource_api_endpoint)
            self._handlers[resource_api_endpoint] = {
                "GET": lambda *_, x=resource_api_endpoint, **__: (
                    self.current_state_get(x, api_version, kind)
                ),
                "PUT": lambda body, *_, x=resource_api_endpoint, **__: (
                    self.current_state_put(x, api_version, kind, body)
                ),
                "PATCH": lambda body, *_, x=resource_api_endpoint, **__: (
                    self.current_state_patch(x, api_version, kind, body)
                ),
                "DELETE": lambda *_, x=resource_api_endpoint, **__: (
                    self.current_state_delete(x, api_version, kind)
                ),
                "WATCH": lambda resource_path, body, *_, **__: (
                    self.current_state_watch(
                        resource_path, api_version, kind, resourced=True
                    )
                ),
            }

            # Add status PUT
            status_resource_api_endpoint = "/".join([resource_api_endpoint, "status"])
            self._handlers[status_resource_api_endpoint] = {
                "PUT": lambda body, *_, x=status_resource_api_endpoint, **__: (
                    self.current_state_put(
                        x,
                        api_version,
                        kind,
                        body,
                        is_status=True,
                    )
                )
            }

    def _get_object_state(self, method, namespace, kind, api_version, name):
        create = method in ["PUT", "PATCH", "POST"]
        if create:
            content = (
                self.__cluster_state.setdefault(namespace, {})
                .setdefault(kind, {})
                .setdefault(api_version, {})
                .get(name, {})
            )
        else:
            content = (
                self.__cluster_state.get(namespace, {})
                .get(kind, {})
                .get(api_version, {})
                .get(name)
            )

        # If it's a callable, call it!
        if callable(content):
            log.debug2("Making callable resource content")
            content = content(
                method=method,
                namespace=namespace,
                kind=kind,
                api_version=api_version,
                name=name,
            )

            # Add the defaults and handle the case where it's a tuple with a
            # status code
            if isinstance(content, tuple):
                body, status = content
                log.debug3("Handling tuple content with status [%s]", status)
                self._add_resource_defaults(body, namespace, kind, api_version, name)
                content = (body, status)
            else:
                self._add_resource_defaults(content, namespace, kind, api_version, name)
            log.debug3("Content: %s", content)
        return content

    def _list_object_state(self, method, namespace, kind, api_version):
        if method != "GET":
            return ([], 405)

        content = (
            self.__cluster_state.setdefault(namespace, {})
            .setdefault(kind, {})
            .setdefault(api_version, {})
        )

        resource_list = []
        return_status = 200
        for resource_name in content:
            resource = content[resource_name]

            # If it's a callable, call it!
            if callable(resource):
                log.debug2("Making callable resource content")
                resource = resource(
                    method=method,
                    namespace=namespace,
                    kind=kind,
                    api_version=api_version,
                    name=resource_name,
                )

            # Add the defaults and handle the case where it's a tuple with a
            # status code
            if isinstance(resource, tuple):
                resource, status = resource
                if status == 403:
                    return_status = 403
                    break
                self._add_resource_defaults(
                    resource, namespace, kind, api_version, resource_name
                )
            else:
                self._add_resource_defaults(
                    resource, namespace, kind, api_version, resource_name
                )

            log.debug3("Resource: %s", content)
            resource_list.append(resource)

        content = {"apiVersion": "v1", "kind": "List", "items": resource_list}
        return (content, return_status)

    def _update_object_current_state(self, namespace, kind, api_version, name, state):
        """Helper function to update a resource in the cluster and update all watch queues"""
        self.__cluster_state.setdefault(namespace, {}).setdefault(kind, {}).setdefault(
            api_version, {}
        )

        # Get the event type based on if name already exists
        event_type = KubeEventType.ADDED
        if name in self.__cluster_state[namespace][kind][api_version]:
            event_type = KubeEventType.MODIFIED

        # Update the cluster state and add resource_path if it doesn't already exist
        self.__cluster_state[namespace][kind][api_version][name] = state
        self._add_handlers_for_resource(
            namespace=namespace, kind=kind, api_version=api_version, name=name
        )
        self._update_watch_queues(event_type, state)

    def _delete_object_state(self, namespace, kind, api_version, name):
        """Helper function to delete a resource in the cluster and update all watch queues"""

        original_object = self.__cluster_state[namespace][kind][api_version][name]

        del self.__cluster_state[namespace][kind][api_version][name]
        if not self.__cluster_state[namespace][kind][api_version]:
            del self.__cluster_state[namespace][kind][api_version]
        if not self.__cluster_state[namespace][kind]:
            del self.__cluster_state[namespace][kind]
        if not self.__cluster_state[namespace]:
            del self.__cluster_state[namespace]

        self._update_watch_queues(KubeEventType.DELETED, original_object)

        # Remove any endpoint handlers for this resource
        self._remove_handlers_for_resource(namespace, kind, api_version, name)

        return True

    def _add_watch_queue(self, queue):
        with self.__queue_lock:
            log.debug3("Adding watch queue %s", queue)
            self.__watch_queues.add(queue)

    def _remove_watch_queue(self, queue):
        with self.__queue_lock:
            log.debug3("Removing watch queue %s", queue)
            self.__watch_queues.remove(queue)

    def _update_watch_queues(self, event, object):
        with self.__queue_lock:
            log.debug2("Updating watch queues with %s event", event)
            for queue in self.__watch_queues:
                queue.put((event, object))

    @staticmethod
    def _add_resource_defaults(obj_state, namespace, kind, api_version, name):
        obj_state["apiVersion"] = api_version
        obj_state["kind"] = kind
        md = obj_state.setdefault("metadata", {})
        if namespace:
            md["namespace"] = namespace
        md["name"] = name
        last_applied_annotation = annotate_last_applied(obj_state)
        md.setdefault("annotations", {}).update(
            last_applied_annotation["metadata"]["annotations"]
        )

    @classmethod
    def _patch_resource(cls, base, overrides):
        """Merge helper that supports removing elements when the override is set
        to None
        """
        for key, value in overrides.items():
            if value is None and key in base:
                del base[key]

            elif (
                key not in base
                or not isinstance(base[key], dict)
                or not isinstance(value, dict)
            ):
                base[key] = value
            else:
                base[key] = cls._patch_resource(base[key], value)

        return base

    ## Handlers ################################################################

    @classmethod
    def not_found(cls, *_, **__):
        log.debug3("Not Found")
        return cls._make_response(
            {
                "kind": "Status",
                "apiVersion": "v1",
                "metadata": {},
                "status": "Failure",
                "message": "the server could not find the requested resource",
                "reason": "NotFound",
                "details": {},
                "code": 404,
            },
            404,
        )

    def apis(self, *_, **__):
        api_group_list = {"kind": "APIGroupList", "apiVersion": "v1", "groups": []}
        for group_name, api_versions in self._api_group_kinds.items():
            if group_name is None:
                continue
            group = {"name": group_name, "versions": []}
            for api_version in api_versions:
                group["versions"].append(
                    {
                        "groupVersion": f"{group_name}/{api_version}",
                        "version": api_version,
                    }
                )
            group["preferredVersion"] = group["versions"][0]
            api_group_list["groups"].append(group)
        return self._make_response(api_group_list)

    def api_v1(self, *_, **__):
        return self.current_state_crds(None, "v1")

    def current_state_crds(self, group_name, api_version):
        resource_list = {
            "kind": "APIResourceList",
            "apiVersion": "v1",
            "groupVersion": f"{group_name}/{api_version}",
            "resources": [],
        }
        for kind in self._api_group_kinds.get(group_name, {}).get(api_version, []):
            resource_list["resources"].append(
                {
                    "name": f"{kind.lower()}s",
                    "singularName": kind.lower(),
                    "namespaced": True,
                    "kind": kind,
                    "verbs": [
                        "delete",
                        "deletecollection",
                        "get",
                        "list",
                        "patch",
                        "create",
                        "update",
                        "watch",
                    ],
                    "storageVersionHash": base64.b64encode(kind.encode("utf-8")).decode(
                        "utf-8"
                    ),
                }
            )
            resource_list["resources"].append(
                {
                    "name": f"{kind.lower()}s/status",
                    "singularName": "",
                    "namespaced": True,
                    "kind": kind,
                    "verbs": ["get", "patch", "update"],
                }
            )
        return self._make_response(resource_list)

    def current_state_watch(
        self, api_endpoint, api_version, kind, resourced=False, query_params=None
    ):
        # Parse the endpoint for the namespace and name
        endpoint_parts = api_endpoint.split("/")
        namespace = None
        name = None
        if "namespaces" in endpoint_parts:
            namespace = endpoint_parts[endpoint_parts.index("namespaces") + 1]

        if resourced:
            name = endpoint_parts[-1]

        # Return Watch Stream Response
        return MockWatchStreamResponse(
            api_client=self,
            api_version=api_version,
            kind=kind,
            namespace=namespace,
            name=name,
            timeout=(query_params or {}).get("timeoutSeconds"),
        )

    def current_state_get(self, api_endpoint, api_version, kind):
        # Parse the endpoint for the namespace and name
        endpoint_parts = api_endpoint.split("/")
        namespace = ""
        if "namespaces" in endpoint_parts:
            namespace = endpoint_parts[endpoint_parts.index("namespaces") + 1]
        name = endpoint_parts[-1]

        # Look up the resources in the cluster state
        log.debug2(
            "Looking for current state of [%s/%s/%s/%s]",
            namespace,
            kind,
            api_version,
            name,
        )
        content = self._get_object_state(
            method="GET",
            namespace=namespace,
            kind=kind,
            api_version=api_version,
            name=name,
        )
        log.debug4("Content: %s", content)
        if content is not None:
            # If the content includes a status code, make the response with it
            if isinstance(content, tuple):
                return self._make_response(*content)
            return self._make_response(content)
        return self.not_found()

    def current_state_list(self, api_endpoint, api_version, kind):
        # Parse the endpoint for the namespace and name and where the kind is located
        # in endpoint_parts
        endpoint_parts = api_endpoint.split("/")
        namespace = ""
        kind_loc = 1
        if "namespaces" in endpoint_parts:
            kind_loc = endpoint_parts.index("namespaces") + 2
            namespace = endpoint_parts[kind_loc - 1]
        else:
            version_split = api_version.split("/")
            # 2 for ["","api"] and then add length of api_version split which would be
            # 2 for resources with a group and 1 without e.g. v1 = 1 and foo.bar.com/v1 would be 2
            kind_loc = 2 + len(version_split)

        # If Api was trying to get a specific resource and not list then return 404
        # as object must not have been found. This is checked by seeing if the kind
        # is at the end of the endpoint_parts
        if kind_loc != len(endpoint_parts) - 1:
            return self.not_found()

        # Look up the resources in the cluster state
        log.debug2(
            "Listing current state of [%s/%s/%s]",
            namespace,
            kind,
            api_version,
        )
        content = self._list_object_state(
            method="GET",
            namespace=namespace,
            kind=kind,
            api_version=api_version,
        )
        log.debug4("Content: %s", content)
        if content is not None:
            # If the content includes a status code, make the response with it
            if isinstance(content, tuple):
                return self._make_response(*content)
            return self._make_response(content)
        return self.not_found()

    def current_state_put(self, api_endpoint, api_version, kind, body, is_status=False):
        # Parse the endpoint for the namespace and name
        endpoint_parts = api_endpoint.split("/")
        namespace = ""
        if "namespaces" in endpoint_parts:
            namespace = endpoint_parts[endpoint_parts.index("namespaces") + 1]
        name = endpoint_parts[-1] if not is_status else endpoint_parts[-2]

        # Look up the resources in the cluster state
        log.debug2(
            "Looking for current state of [%s/%s/%s/%s]",
            namespace,
            kind,
            api_version,
            name,
        )
        content = self._get_object_state(
            method="PUT",
            namespace=namespace,
            kind=kind,
            api_version=api_version,
            name=name,
        )
        log.debug3("Current Content: %s", content)

        # If the content has a status code, unpack it
        status_code = 200
        if isinstance(content, tuple):
            content, status_code = content

        # If it's a non-200 status code, don't make the update
        if status_code != 200:
            return self._make_response(content, status_code)

        # If this is a status, we are only updating the status and keeping the
        # existing content
        if is_status:
            content.update({"status": body.get("status", {})})
            updated_content = content
        else:
            if "status" in body:
                del body["status"]
            updated_content = body
        log.debug3(
            "Updating [%s/%s/%s/%s] with body: %s",
            namespace,
            kind,
            api_version,
            name,
            updated_content,
        )
        self._update_object_current_state(
            namespace, kind, api_version, name, updated_content
        )

        return self._make_response(updated_content, status_code)

    def current_state_patch(self, api_endpoint, api_version, kind, body):
        # Parse the endpoint for the namespace and name
        endpoint_parts = api_endpoint.split("/")
        namespace = ""
        if "namespaces" in endpoint_parts:
            namespace = endpoint_parts[endpoint_parts.index("namespaces") + 1]
        name = endpoint_parts[-1]

        # Look up the resources in the cluster state
        log.debug2(
            "Looking for current state of [%s/%s/%s/%s]",
            namespace,
            kind,
            api_version,
            name,
        )
        content = self._get_object_state(
            method="PATCH",
            namespace=namespace,
            kind=kind,
            api_version=api_version,
            name=name,
        )
        log.debug3("Current Content: %s", content)
        log.debug3("Update body: %s", body)

        # If the content has a status code, unpack it
        status_code = 200
        if isinstance(content, tuple):
            content, status_code = content

        # If it's a non-200 status code, don't make the update
        if status_code != 200:
            return self._make_response(content, status_code)

        # Merge in the new body
        if "status" in body:
            del body["status"]
        log.debug3(
            "Updating [%s/%s/%s/%s] with body: %s",
            namespace,
            kind,
            api_version,
            name,
            body,
        )
        updated_content = self._patch_resource(content, body)
        log.debug3("Updated content: %s", updated_content)
        self._update_object_current_state(
            namespace, kind, api_version, name, updated_content
        )

        return self._make_response(updated_content, status_code)

    def current_state_post(self, api_endpoint, api_version, kind, body):
        log.debug2("Creating current state for [%s]", api_endpoint)

        # Parse the endpoint and body for the namespace and name
        endpoint_parts = api_endpoint.split("/")
        namespace = ""
        if "namespaces" in endpoint_parts:
            namespace = endpoint_parts[endpoint_parts.index("namespaces") + 1]
        name = body.get("metadata", {}).get("name")

        # Look up the resources in the cluster state
        log.debug2(
            "Looking for current state of [%s/%s/%s/%s]",
            namespace,
            kind,
            api_version,
            name,
        )
        content = self._get_object_state(
            method="POST",
            namespace=namespace,
            kind=kind,
            api_version=api_version,
            name=name,
        )
        log.debug3("Current Content: %s", content)

        # If the content has a status code, unpack it
        status_code = 200
        if isinstance(content, tuple):
            content, status_code = content

        # If it's a non-200 status code, don't make the update
        if status_code != 200:
            return self._make_response(content, status_code)

        # Overwrite the body
        log.debug3("Overwrite content: %s", body)
        self._update_object_current_state(namespace, kind, api_version, name, body)

        return self._make_response(body, status_code)

    def current_state_delete(self, api_endpoint, api_version, kind):
        # Parse the endpoint for the namespace and name
        endpoint_parts = api_endpoint.split("/")
        namespace = ""
        if "namespaces" in endpoint_parts:
            namespace = endpoint_parts[endpoint_parts.index("namespaces") + 1]
        name = endpoint_parts[-1]

        # Look up the resources in the cluster state
        log.debug2(
            "Looking for current state of [%s/%s/%s/%s]",
            namespace,
            kind,
            api_version,
            name,
        )

        content = self._get_object_state("DELETE", namespace, kind, api_version, name)
        response_code = 200
        if isinstance(content, tuple):
            content, response_code = content
        deleted = content is not None

        if content is not None:
            self._delete_object_state(
                namespace=namespace,
                kind=kind,
                api_version=api_version,
                name=name,
            )
        response_content = {
            "kind": "status",
            "apiVersion": "v1",
            "metadata": {},
            "details": {
                "name": name,
                "kind": kind,
            },
        }

        if deleted:
            response_content["status"] = "Success"
            response_content["details"]["uid"] = "Hope nothing uses this"
        else:
            response_content["status"] = "Failure"
            response_content["message"] = f'{kind} "{name}" not found'
            response_code = 404
        return self._make_response(response_content, response_code)


@contextmanager
def mock_kub_client_constructor(*args, **kwargs):
    """Context manager to patch the api client"""
    log.debug("Getting mocked client")
    client = MockKubClient(*args, **kwargs)
    log.debug("Mock client complete")
    with mock.patch(
        "kubernetes.config.new_client_from_config",
        return_value=client,
    ):
        yield client
