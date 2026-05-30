#!/bin/bash
set -e

NS=gpu-telemetry
TOKEN=$(kubectl -n $NS get secret influxdb-auth -o jsonpath='{.data.admin-token}' | base64 -d)

echo "▶ Replica counts:"
kubectl -n $NS get deploy streamer collector

echo ""
echo "▶ Pod health:"
kubectl -n $NS get pods -l 'app in (streamer,collector)' \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.phase}{"\t"}{.status.containerStatuses[0].ready}{"\n"}{end}'

echo ""
echo "▶ Collector connection logs (last line each):"
for pod in $(kubectl -n $NS get pods -l app=collector -o name); do
    last=$(kubectl -n $NS logs $pod --tail=1 2>/dev/null)
    echo "  $pod: $last"
done

echo ""
echo "▶ Write rate (last 5 min):"
kubectl -n $NS exec deploy/influxdb -- influx query \
    --org ai_org --token $TOKEN \
    'from(bucket:"tel_db") |> range(start:-5m) |> count()'

echo ""
echo "▶ Continuity check for gpu_id=0 (gaps = lost data):"
kubectl -n $NS exec deploy/influxdb -- influx query \
    --org ai_org --token $TOKEN \
    'from(bucket:"tel_db") |> range(start:-5m) |> filter(fn:(r)=>r.gpu_id=="0") |> aggregateWindow(every:30s, fn:count, createEmpty:true)'