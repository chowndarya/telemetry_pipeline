<b>Telemetry Pipeline — Installation Guide (macOS)</b>

This guide walks you through installing, deploying, testing, and uninstalling the GPU Telemetry Pipeline on macOS using KinD (Kubernetes in Docker), Podman, and Helm.



Table of Contents

Prerequisites
KinD Cluster Setup
Configuration
Build & Deploy
Verify the Deployment
Running Tests
Generating the OpenAPI Spec
Uninstall
Troubleshooting


<b><h3>1. Prerequisites</h3></b>

Install the following tools on your macOS system before proceeding:


Tool	Version	Install Command
Podman	4.x or higher	brew install podman
KinD	0.20+	brew install kind
kubectl	1.27+	brew install kubectl
Helm	3.8+	brew install helm
Go	1.23+ (only for tests/OpenAPI)	brew install go
Make	preinstalled on macOS	—

Initialize Podman Machine

If running Podman for the first time on macOS:

```bash
podman machine init
podman machine start
```

Note: This setup uses Podman for image builds (not Docker). If you prefer Docker, see the Using Docker Instead of Podman note in the Troubleshooting section.


<b><h3>2. KinD Cluster Setup</h3></b>

The Makefile expects a KinD cluster named kind-cluster.


Create the cluster:

```bash
kind create cluster --name kind-cluster
```
Verify it's running:

```bash
kind get clusters
kubectl cluster-info --context kind-kind-cluster
```

If your cluster has a different name, update the --name value in the load target of the Makefile accordingly.


<b><h3>3. Configuration</h3></b>

Before deploying, review and adjust the Helm chart values to match your environment.


Edit gpu-telemetry-chart/values.yaml to customize:


Replica counts for each component (apis, queue, collector, streamer)
Resource requests and limits (CPU, memory)
Storage size for the queue StatefulSet (default 1Gi)
Image registry, tag, and pull policy
Service ports and types

Example snippet:


```yaml
namespace: gpu-telemetry

image:
  registry: localhost
  tag: latest
  pullPolicy: IfNotPresent

queue:
  replicas: 1
  storageSize: 1Gi
  maxQueueSize: 10000
```

<b><h3>4. Build & Deploy</h3></b>

The Makefile provides a one-shot make all target that performs the full lifecycle: build → save → load → deploy.


Run the full deployment:
```bash
make all
```
This will:


Build container images for all four components (telemetry_apis, telemetry_queue, telemetry_collector, telemetry_streamer) using Podman with platform auto-detection.
Save each image as a .tar archive.
Load the archives into the KinD cluster (kind-cluster).
Deploy the Helm chart gpu-telemetry into the gpu-telemetry namespace.

Run individual steps (optional):

```bash
make build      # Build images only
make push       # Save images to .tar files
make load       # Load images into KinD cluster
make deploy     # Helm install/upgrade
make clean      # Remove generated .tar files
```

Verify pods are running:

kubectl get pods -n gpu-telemetry

All pods (telemetry-apis, telemetry-queue, telemetry-collector, telemetry-streamer, influxdb) should reach Running state within 1–2 minutes.



<b><h3>5. Verify the Deployment</h3></b>

Step 1: Port-forward the API service

The telemetry API is exposed as a ClusterIP service. To access it from your Mac, run:

kubectl port-forward svc/telemetry-apis 8080:8080 -n gpu-telemetry

Keep this terminal running.


Step 2: Open the Swagger UI

In your browser, navigate to:

http://localhost:8080/swagger/index.html

You should see the interactive Swagger documentation listing all telemetry APIs.


Step 3: List available GPUs

Call the GPUs endpoint to retrieve unique GPU IDs:

curl http://localhost:8080/api/v1/gpus

Or open in browser: http://localhost:8080/api/v1/gpus


You'll receive a JSON response with a list of GPU IDs. Pick one of them.


Step 4: Fetch telemetry for a specific GPU

Replace {id} with the GPU ID from the previous step:

curl http://localhost:8080/api/v1/gpus/{id}/telemetry

A successful response containing a list of metrics confirms the end-to-end installation is working — the streamer is producing data, the queue is buffering, the collector is storing into InfluxDB, and the API is reading back successfully.



<b><h3>6. Running Tests</h3></b>

The Makefile includes targets for unit, integration, and stress tests.


Unit tests:

```bash
make test                    # Run all unit tests (verbose)
make test-short              # Run only short tests
make test-streamer           # Test only the streamer component
make test-collector          # Test only the collector component
make test-apis               # Test only the API component
```

Integration tests (requires Podman/Docker):

make test-integration

Code coverage:


make coverage                # Generate coverage report
make coverage-html           # Generate HTML report and open in browser
make coverage-by-package     # Coverage broken down by package
make coverage-threshold      # Fail if coverage < 70%

Stress tests:

make test-stress             # 10 producers + 10 collectors
make test-stress-bench       # With CPU & memory profiling

Full CI pipeline:

make ci                      # deps + lint + coverage + threshold check

View available targets:

make help


<b><h3>7. Generating the OpenAPI Spec</h3></b>

The REST API's OpenAPI specification is auto-generated from Go annotations using `swag`.


Generate the spec:

make openapi

This produces:


telemetry_apis/docs/swagger.json — OpenAPI 2.0 JSON
telemetry_apis/docs/swagger.yaml — OpenAPI 2.0 YAML
telemetry_apis/docs/docs.go — Embedded Go file for the runtime Swagger UI


The swag CLI will be auto-installed if not already present (requires Go).

Note: The generated spec follows OpenAPI 2.0 (formerly Swagger 2.0). If you need OpenAPI 3.x, convert it using `swagger2openapi`.

<b><h3>8. Uninstall</h3></b>

To remove the telemetry pipeline:

helm uninstall gpu-telemetry -n gpu-telemetry
kubectl delete namespace gpu-telemetry

To delete the KinD cluster entirely:

kind delete cluster --name kind-cluster

To clean local image archives:

make clean


<b><h3>9. Troubleshooting</h3></b>

ImagePullBackOff for localhost/... images

Kubernetes is trying to pull images from a remote registry instead of using the locally loaded ones.


Ensure imagePullPolicy: IfNotPresent (or Never) is set in your Helm templates.

Verify the image is loaded in the cluster:

bash

docker exec -it kind-cluster-control-plane crictl images | grep telemetry
If missing, re-run make load.


make: docker: No such file or directory

The Makefile uses podman. If you see this error, ensure Podman is installed and podman machine is started:


bash
podman machine start

no nodes found for cluster "kind"

You have a KinD cluster with a non-default name. Pass the --name flag:


bash
kind load image-archive <file>.tar --name kind-cluster

The Makefile already does this; verify your cluster is named kind-cluster or update the Makefile.


Pod stuck in CrashLoopBackOff (queue component)

Likely a BoltDB volume permission issue. Check logs:


bash
kubectl logs -n gpu-telemetry telemetry-queue-0

Ensure your Helm StatefulSet has securityContext.fsGroup: 1000 set, and that the BoltDB path exists at /data/queue.db.


Using Docker Instead of Podman

If you prefer Docker, replace podman with docker in the Makefile's build and push (save) targets. The kind load image-archive command works identically with either runtime.


Cannot access Swagger at http://localhost:8080

Ensure kubectl port-forward svc/telemetry-apis 8080:8080 -n gpu-telemetry is running in a separate terminal. If port 8080 is already in use, choose a different local port:


bash
Copy Code
kubectl port-forward svc/telemetry-apis 9090:8080 -n gpu-telemetry
# Then open http://localhost:9090/swagger/index.html


Quick Reference

Action	Command
Full deployment	make all
List pods	kubectl get pods -n gpu-telemetry
Access Swagger	kubectl port-forward svc/telemetry-apis 8080:8080 -n gpu-telemetry
Run unit tests	make test
Run stress tests	make test-stress
Generate OpenAPI spec	make openapi
Uninstall	helm uninstall gpu-telemetry -n gpu-telemetry
Delete cluster	kind delete cluster --name kind-cluster


For further assistance or customization, please contact the project maintainers.

If you need further assistance or customization, please reach out.
