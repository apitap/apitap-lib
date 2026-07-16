#!/usr/bin/env bash
# Provision the three dedicated GCP VMs used by the benchmark.
# Source DB + Destination DB on n2-highcpu-32; the tool host on n2-highcpu-16.
# All three land on the default VPC and talk over internal IPs — no public hop
# in the data path.
#
#   PROJECT=my-proj ZONE=us-east1-b ./provision.sh
#
# After it prints the internal IPs, plug them into setup-dbs.sh / the harnesses.
set -euo pipefail
PROJECT="${PROJECT:?set PROJECT}"
ZONE="${ZONE:-us-east1-b}"
IMG=(--image-family=ubuntu-2204-lts --image-project=ubuntu-os-cloud)

gcloud compute instances create bench-source bench-dest \
  --project="$PROJECT" --zone="$ZONE" --machine-type=n2-highcpu-32 \
  "${IMG[@]}" --boot-disk-size=100GB --boot-disk-type=pd-ssd

gcloud compute instances create bench-ingest \
  --project="$PROJECT" --zone="$ZONE" --machine-type=n2-highcpu-16 \
  "${IMG[@]}" --boot-disk-size=50GB --boot-disk-type=pd-ssd

echo "== internal IPs (use these; never the public ones) =="
gcloud compute instances list --project="$PROJECT" \
  --filter="name~bench-" \
  --format="table(name, networkInterfaces[0].networkIP, machineType.basename())"
