# Session Config

Each reconciliation merges a backend `config` into the
[Session](architecture.md#session) that components read with `session.config`.
The config is assembled from three sources, listed here from lowest to highest
precedence:

1. **Controller defaults** (`Controller(config_defaults=...)`): authored in the
   operator's code. This is the only trusted source.
2. **The `oper8.org/config-defaults` annotation**: names a ConfigMap in the CR's
   namespace whose `data` holds additional defaults.
3. **The CR's top-level `configOverrides` field**: merged with the highest
   precedence.

## Trust model

The annotation and `configOverrides` can be written by anyone who can edit the
CR (or, for the annotation, the ConfigMap it names in the CR's namespace), so
oper8 does not trust their contents. Config values are applied by the operator
with the operator's own permissions, so untrusted config must not be able to
direct *where* the operator deploys resources.

The following rule is always enforced:

- **CR-controlled config may not change resource placement:** A `namespace` key
  in any per-component section of `configOverrides` (or of the config from a
  `config-defaults` ConfigMap) is rejected. Since `configOverrides` is merged
  with the highest precedence, rejecting these keys outright is also what keeps
  a CR from silently overriding a namespace that the operator set in its
  controller defaults. Violating this rule fails the reconciliation with a
  `ConfigError`, which is reported in the CR's status.

Because that check covers every untrusted source, the values that remain in the
merged config are trusted operator choices:

- **Per-component namespaces are a controller-config feature:** When a
  component's config section sets a `namespace`
  (e.g. `config_defaults={"my-component": {"namespace": "other-ns"}}`),
  `Oper8xComponent` deploys that component's rendered resources in the given
  namespace. The value can only originate from the controller defaults, so
  placement always remains an operator decision.

> **NOTE:** Operators that implement their own config-driven placement in
> custom `Component` hooks should apply the same trust boundary: a value from
> `configOverrides` or a `config-defaults` ConfigMap must never decide the
> namespace of a rendered resource. For the same reason, temporary patches may
> not change the identity (including the namespace) of a rendered resource; see
> [temporary patches](temporary_patches.md).
