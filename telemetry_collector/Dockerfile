FROM --platform=$BUILDPLATFORM golang:latest AS builder


WORKDIR /app

# 1. Copy only the dependency files first
COPY go.mod go.sum ./

# 2. Download dependencies (this respects the 'replace' in go.mod)
RUN go mod download

# 3. Copy the rest of the source code
COPY . .

# 4. Build with -mod=readonly
# This flag ensures the build fails if go.mod is inconsistent 
# instead of trying to download/change things during build.
RUN CGO_ENABLED=0 GOOS=linux go build -mod=readonly -o /collector ./telemetry_collector/main.go

FROM alpine:latest
RUN apk add --no-cache ca-certificates
WORKDIR /root/
COPY --from=builder /collector .

ENV INFLUXDB_URL=influxdb:8181
ENV GRPC_SERVER_ADDR=telemetry-queue:50051

CMD ["./collector"]