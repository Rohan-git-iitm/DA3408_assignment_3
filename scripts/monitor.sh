#!/bin/bash
trap "exit" INT
out=$1
shift
while true; do
  docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}}" "$@" \
    | sed "s/^/$(date +%s),/" >> "$out"
done