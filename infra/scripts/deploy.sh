#!/usr/bin/env bash
# PLANNED — wraps `az deployment group create` against infra/bicep/main.bicep
# Usage: ./deploy.sh <dev|prod>
set -euo pipefail
ENV="${1:-dev}"
echo "TODO: az group create + az deployment group create for env=${ENV}"
