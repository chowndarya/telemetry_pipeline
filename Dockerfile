FROM influxdb:latest
# Override default port to match your code (8181)
EXPOSE 8181
# You can add custom initialization scripts here if needed