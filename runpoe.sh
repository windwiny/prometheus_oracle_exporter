#!/bin/sh

while true; do
    ./prometheus_oracle_exporter $*
    if [[ $? != 123 ]]; then
        break
    fi
done
