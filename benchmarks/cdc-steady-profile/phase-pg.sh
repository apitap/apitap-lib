#!/usr/bin/env bash
# Phase driver: pg 0.5-CPU steady rounds. Round 1 carries a 40 s perf -g record.
set -uo pipefail
C="$HOME/apitap-lib/benchmarks/cdc-steady-profile"
bash "$C/campaign.sh" prep pg
bash "$C/campaign.sh" boot pg 0.5
PERF_G=1 bash "$C/campaign.sh" steady pg s-pg-r1 0.5 100000 60 210 40
bash "$C/campaign.sh" steady pg s-pg-r2 0.5 100000 60 210
bash "$C/campaign.sh" steady pg s-pg-r3 0.5 100000 60 210
bash "$C/campaign.sh" purgelogs pg
echo PHASE_PG_DONE
