FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Ensure swagger docs are included in the build context
RUN CGO_ENABLED=0 GOOS=linux go build -o /api-gateway ./telemetry_apis/main.go

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /api-gateway .
# Copy the generated docs so Swagger UI works
COPY --from=builder /app/telemetry_apis/docs ./docs
EXPOSE 8080
ENV INFLUXDB_URL=http://influxdb:8181
CMD ["./api-gateway"]