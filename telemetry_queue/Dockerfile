# Use --platform to match your Mac's architecture (ARM64)
# Use 1.23-alpine (current latest stable) to satisfy newer go.mod requirements
FROM --platform=$BUILDPLATFORM golang:latest AS builder

WORKDIR /app

# This environment variable allows Go to download a newer toolchain 
# if your go.mod specifically requests a version higher than the image
ENV GOTOOLCHAIN=auto

# Copy go.mod and sum from root
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire project to resolve local proto dependencies
COPY . .

# Build the queue binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /queue ./telemetry_queue

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /queue .
EXPOSE 50051
CMD ["./queue"]