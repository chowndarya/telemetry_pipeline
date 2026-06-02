Telemetry System Installation and Testing Guide for macOS

This guide provides step-by-step instructions to install, deploy, and test the telemetry system on macOS using KinD (Kubernetes in Docker), Docker CLI, Podman, and Helm.



Prerequisites

Container Tools:

Ensure Docker CLI is installed on your macOS system.
Podman is recommended for saving images, but if you prefer, you can use Docker commands instead.
You must have a KinD cluster running. If you have your own KinD cluster without Podman, adjust the commands accordingly.
Helm:

Install Helm (version 3.8.0 or higher) for deploying the telemetry system.
Make:

Ensure make is installed to run the provided Makefile commands.


Configuration

Before deployment, review and modify Helm chart values as needed:
Adjust replica counts (default is 3) for your environment.
Modify resource requests and limits according to your system capacity.
Customize any other Helm values in the gpu-telemetry-chart as required.


Installation and Deployment Steps

Build, Save, Load, and Deploy:

Run the following command in the root directory containing the Makefile:

bash
Copy Code
make all
This will:

Build Docker images for all telemetry components with platform auto-detection.
Save the images as tar archives using Podman.
Load the images into your KinD cluster.
Deploy the telemetry system using Helm with production-ready settings.
If you do not use Podman:

Replace podman save commands with docker save in the Makefile.
Adjust image loading commands to match your KinD cluster name if different from kind-cluster.


Testing the Deployment

Access the Swagger UI:

Open your browser and navigate to:

Copy Code
http://localhost:8080/swagger/index.html#
Verify Telemetry Data:

Call the API to get unique GPU IDs:

Copy Code
http://localhost:8080/api/v1/gpus
Select a GPU ID from the response.

Use the selected GPU ID to query telemetry metrics:

Copy Code
http://localhost:8080/api/v1/gpus/{id}/telemetry
A successful response with a list of metrics confirms the system is installed and running correctly.



Notes

If you have a custom KinD cluster name, update the Makefile's kind load image-archive commands with your cluster name using the --name flag.
Adjust Helm values in the deploy target of the Makefile to fit your production environment.
For any issues with image pushing or loading, verify your container runtime and KinD cluster configurations.


This setup automates the full lifecycle from image build to deployment and verification on macOS, ensuring a smooth installation experience.


If you need further assistance or customization, please reach out.
