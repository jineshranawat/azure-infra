#!/usr/bin/env bash
# Class-1 deploy script — creates the resource group (with tags) then deploys Bicep.
# Guardrail-first: RG + tags → Bicep handles budget → KV → storage → RBAC.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load .env when present (LEARNER, OWNER_EMAIL, LOCATION, AZURE_SUBSCRIPTION_ID).
if [[ -f "${SCRIPT_DIR}/.env" ]]; then
  # shellcheck disable=SC1091
  source "${SCRIPT_DIR}/.env"
fi

LEARNER="${LEARNER:-demo}"
OWNER_EMAIL="${OWNER_EMAIL:-learner@example.com}"
LOCATION="${LOCATION:-uksouth}"
SUBSCRIPTION_ID="${AZURE_SUBSCRIPTION_ID:-}"

if [[ "${LOCATION}" != "uksouth" && "${LOCATION}" != "ukwest" ]]; then
  echo "ERROR: LOCATION must be uksouth or ukwest (UK residency requirement)." >&2
  exit 1
fi

RG_NAME="rg-${LEARNER}-class1"

# Mandatory governance tags — mirrored in Bicep and provision.py.
TAGS=(
  "env=training"
  "owner=${OWNER_EMAIL}"
  "costcentre=boe-data-enablement"
  "data-class=training-synthetic"
  "course=azure-etl-boe"
  "class=class-1"
  "auto-teardown=nightly"
)

TAG_ARGS=()
for tag in "${TAGS[@]}"; do
  TAG_ARGS+=(--tags "$tag")
done

if [[ -n "${SUBSCRIPTION_ID}" ]]; then
  az account set --subscription "${SUBSCRIPTION_ID}"
fi

echo "Deriving principalObjectId from signed-in user..."
PRINCIPAL_OBJECT_ID="$(az ad signed-in-user show --query id -o tsv)"

# Budget timePeriod.startDate must be the 1st of the current month.
BUDGET_START_DATE="$(date -u +%Y-%m-01)"

echo "Creating resource group ${RG_NAME} in ${LOCATION}..."
az group create \
  --name "${RG_NAME}" \
  --location "${LOCATION}" \
  "${TAG_ARGS[@]}" \
  --output none

echo "Deploying Bicep template to ${RG_NAME}..."
az deployment group create \
  --resource-group "${RG_NAME}" \
  --template-file "${SCRIPT_DIR}/infra/main.bicep" \
  --parameters \
    location="${LOCATION}" \
    learner="${LEARNER}" \
    ownerEmail="${OWNER_EMAIL}" \
    principalObjectId="${PRINCIPAL_OBJECT_ID}" \
    budgetStartDate="${BUDGET_START_DATE}" \
  --output table

echo "Deployment complete. Resource group: ${RG_NAME}"
