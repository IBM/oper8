# Makefile for oper8-go (rewrite/)
# Run all targets from this directory: make <target>

CONTROLLER_GEN ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.17.0
VERSION        ?= 0.0.1
IMG            ?= controller:latest
BUNDLE_IMG     ?= bundle:$(VERSION)
OPERATOR_SDK   ?= operator-sdk

.PHONY: all
all: generate manifests build

# ── Code generation ────────────────────────────────────────────────────────────

.PHONY: generate
generate: ## Generate DeepCopy methods (api/v1alpha1/zz_generated.deepcopy.go)
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: manifests
manifests: ## Generate CRD YAML into config/crd/bases/
	$(CONTROLLER_GEN) crd paths="./api/..." output:crd:artifacts:config=config/crd/bases

.PHONY: manifests-verify
manifests-verify: manifests ## Verify CRDs are up to date (fails if git dirty — used in CI)
	git diff --exit-code config/crd/

# ── Build ──────────────────────────────────────────────────────────────────────

.PHONY: build
build: ## Compile all packages
	go build ./...

.PHONY: test
test: ## Run all tests with race detector
	go test -race ./...

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run ./...

# ── OLM bundle ─────────────────────────────────────────────────────────────────

.PHONY: bundle
bundle: manifests ## Generate OLM bundle under bundle/
	$(OPERATOR_SDK) generate kustomize manifests -q
	cd config/manager && kustomize edit set image controller=$(IMG)
	kustomize build config/manifests | \
		$(OPERATOR_SDK) generate bundle \
			--overwrite \
			--version $(VERSION) \
			--channels alpha \
			--default-channel alpha
	$(OPERATOR_SDK) bundle validate ./bundle

.PHONY: bundle-build
bundle-build: ## Build the bundle image
	docker build -f bundle/Dockerfile -t $(BUNDLE_IMG) .

.PHONY: bundle-push
bundle-push: ## Push the bundle image
	docker push $(BUNDLE_IMG)

# ── Help ───────────────────────────────────────────────────────────────────────

.PHONY: help
help: ## Print this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
