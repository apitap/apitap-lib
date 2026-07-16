#!/usr/bin/env bash
# Delete the three benchmark VMs (boot disks auto-delete). Run between rounds
# and when finished — an idle n2-highcpu-32 is not free.
#   PROJECT=my-proj ZONE=us-east1-b ./teardown.sh
set -euo pipefail
PROJECT="${PROJECT:?set PROJECT}"
ZONE="${ZONE:-us-east1-b}"
gcloud compute instances delete bench-source bench-dest bench-ingest \
  --project="$PROJECT" --zone="$ZONE" --quiet
gcloud compute instances list --project="$PROJECT" --filter="name~bench-" || true
