#!/usr/bin/env bash
# PLANNED — deletes the resource group for the given environment.
# Usage: ./teardown.sh <dev|prod>
set -euo pipefail
ENV="${1:-dev}"
echo "TODO: az group delete --name rg-iot-pipeline-${ENV} --yes"
