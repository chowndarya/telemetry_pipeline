FROM --platform=$BUILDPLATFORM golang:latest AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /streamer ./telemetry_streamer/main.go

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /streamer .
# Copy the CSV file into the container
COPY telemetry_streamer/dcgm_metrics_20250718_*.csv ./metrics.csv
# Default Env Vars (Can be overridden in K8s/Kind)
ENV CSV_FILE_PATH=/root/metrics.csv
ENV GRPC_SERVER_ADDR="localhost:50051"
CMD ["./streamer"]