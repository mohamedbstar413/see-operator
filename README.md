# see-op

`see-op` is a Kubernetes operator built with Kubebuilder that automates blackbox-style probing for workloads in selected namespaces.

It watches a custom resource named `SeeOperator` and does three main things:

- Deploys and manages the supporting blackbox exporter components when `spec.blackboxUrl` is not provided.
- Discovers Services and Endpoints in the namespaces you choose, then creates Prometheus `Probe` resources for pods that expose HTTP liveness probes.
- Cleans up managed probes when the `SeeOperator` custom resource is deleted.

## Features

- Kubebuilder-based controller written in Go.
- Custom resource: `seeoperators.see-operator.example.com`.
- Creates `Probe` objects from detected pod liveness probe paths.
- Supports either an external blackbox exporter URL or a built-in exporter deployed by the operator.
- Uses a finalizer so probe resources are removed during deletion.
- Includes controller and envtest-based test suites.

## How It Works

At a high level, the controller:

1. Reads the `SeeOperator` custom resource.
2. Adds a finalizer if needed.
3. Deploys or reuses a blackbox exporter endpoint.
4. Scans the namespaces listed in `spec.namespacesToMonitor`.
5. Creates Prometheus `Probe` resources for matching endpoints.
6. Updates status to track monitored namespaces and created probes.
7. Removes probes during cleanup when the custom resource is deleted.

## Spec

The main fields in the sample CR are:

- `namespacesToMonitor`: namespaces to scan for endpoints and pods.
- `blackboxUrl`: optional external blackbox exporter URL.
- `probeSelectorLabels`: labels applied to generated probes.
- `intervalJob`: job interval used by the generated CronJob.
- `dependencies`: toggles optional dependency-related behavior used by the controller.

See the sample manifest at [`config/samples/see-operator_v1_seeoperator.yaml`](./config/samples/see-operator_v1_seeoperator.yaml).

## Repository Layout

- [`cmd/`](./cmd) - manager entrypoint.
- [`api/v1/`](./api/v1) - `SeeOperator` API types.
- [`internal/controller/`](./internal/controller) - reconciliation logic and tests.
- [`internal/manifests/`](./internal/manifests) - embedded YAML templates for generated resources.
- [`config/`](./config) - CRDs, RBAC, sample manifests, and deployment config.

## Prerequisites

- Go 1.24+
- Docker or another compatible container runtime
- kubectl
- A Kubernetes cluster

If you want to run the controller tests that use envtest, you also need the Kubebuilder test binaries available locally.

## Local Development

Install helper binaries:

```sh
make manifests generate
make fmt vet
```

Run the controller locally:

```sh
make run
```

Run unit and controller tests:

```sh
make test
```

## Deploying To Kubernetes

Build and push the manager image:

```sh
make docker-build docker-push IMG=<your-registry>/see-op:tag
```

Install the CRDs:

```sh
make install
```

Deploy the controller:

```sh
make deploy IMG=<your-registry>/see-op:tag
```

Create a sample `SeeOperator` instance:

```sh
kubectl apply -k config/samples/
```

## Uninstall

Remove sample resources:

```sh
kubectl delete -k config/samples/
```

Remove the CRDs:

```sh
make uninstall
```

Remove the controller:

```sh
make undeploy
```

## Build An Installer Bundle

Generate a single YAML bundle for distribution:

```sh
make build-installer IMG=<your-registry>/see-op:tag
```

The resulting bundle is written to `dist/install.yaml`.

## Testing Notes

The repository includes envtest-based controller tests under [`internal/controller/`](./internal/controller) and end-to-end tests under [`test/e2e/`](./test/e2e).

If `make test` fails because envtest binaries are missing, install the required Kubebuilder assets first.

## License

Apache License 2.0. See the repository license for full details.
