#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
docker build -t group17/temperature-api:final-v2 services/api
docker build -t group17/temperature-consumer:final-v4 services/consumer
docker build -t group17/temperature-notifier:final-v3 services/notifier
