# Temporary Patches

Temporary patches let the users of an operator correct the resources that the operator renders for a CR without waiting for a new release of the operator (e.g. pinning an image tag). A patch is defined by a `TemporaryPatch` resource that lives in the same namespace as the CR it targets. The `TemporaryPatchController` records the patch on the target CR with the `oper8.org/temporary-patches` annotation, and the target CR's controller applies the patch to its rendered resources on the next reconciliation.

```yaml
apiVersion: oper8.org/v1
kind: TemporaryPatch
metadata:
  name: pin-image
  namespace: my-namespace
spec:
  apiVersion: foo.bar.com/v1
  kind: Widget
  name: my-widget
  patchType: patchStrategicMerge # or patchJson6902
  patch:
    my_component:
      my_deployment:
        spec:
          template:
            spec:
              containers:
                - name: server
                  image: my-image:1.2.3
```

The top-level keys of `spec.patch` match a resource's internal name (`<component name>.<resource name>`), and the patch is applied to that resource.

## Trust model

The `oper8.org/temporary-patches` annotation can be written by anyone who can edit the CR, so oper8 does not trust its contents. This matches the purpose of an operator: a tenant controls the logical entity (the CR) without needing permission to every kind that the operator manages on their behalf.

Temporary patches can change the non-identity fields of the resources that the operator renders for that CR. Those changes are applied with the operator's own permissions, so they are limited to the kinds and namespaces that the operator's ServiceAccount can already write.

The following rules are always enforced:

- **Identity is immutable:** A patch may not change the `apiVersion`, `kind`, `metadata.name`, or `metadata.namespace` of a rendered resource. JSON 6902 operations that touch these fields (including their parents, such as `/metadata` or the document root) are rejected, and the identity of each resource is compared before and after patching. A resource always deploys (or is deleted) with the identity its component rendered.
- **Patches must target the CR:** The patch resource's `spec.apiVersion` group, `spec.kind`, and `spec.name` must match the CR being reconciled. The version is not compared, so patches still apply when a CRD version is converted.
- **A CR is never its own patch source:** The annotation may not name a patch with the same group and kind as the CR.
- **Patches must be well formed:** The fetched patch must have the `apiVersion` and `kind` named in the annotation, a supported `spec.patchType`, and a `spec.patch` mapping.

Violating any of these rules fails the reconciliation with a `ConfigError`, which is reported in the CR's status.

## Allowed patch kinds

By default, the kinds that may be used as patch sources are discovered automatically from the `TemporaryPatchController` class and all of its subclasses that are loaded in the operator process (e.g. a custom `MyTemporaryPatch` kind used to satisfy OLM's single-owner requirement). If the patch controller runs in a different process from the controller for the target CR, set the list explicitly:

```yaml
temporary_patch:
  # Entries may be either `kind` or `apiVersion/kind`
  allowed_kinds:
    - my.group.name/v1/MyTemporaryPatch
  # Reject patches whose kind is not allowed (default: false)
  enforce_allowed_kinds: true
```

> **NOTE:** `enforce_allowed_kinds` currently defaults to `false`. A patch kind that is not allowed only logs a warning once per process. This will default to `true` in a future release, so operators that use custom patch kinds should make sure those kinds are discovered or listed in `allowed_kinds`.
