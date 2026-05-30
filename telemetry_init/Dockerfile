FROM debian:bookworm-slim

ARG INFLUX_VERSION=3.9.3
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl ca-certificates tar gzip bash \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://www.influxdata.com/d/install_influxdb3.sh -o /tmp/install.sh \
    && bash /tmp/install.sh </dev/null || true \
    && mv /root/.influxdb/influxdb3 /usr/local/bin/influxdb3 \
    && chmod +x /usr/local/bin/influxdb3 \
    && rm -rf /tmp/install.sh /root/.influxdb

COPY setup.sh /setup.sh
RUN chmod +x /setup.sh

ENTRYPOINT ["/setup.sh"]