#!/usr/bin/env bash
# Phase driver: mysql 0.5-CPU steady rounds. Round 1 carries a 40 s perf -g record.
set -uo pipefail
C="$HOME/apitap-lib/benchmarks/cdc-steady-profile"
bash "$C/campaign.sh" prep my
bash "$C/campaign.sh" boot my 0.5
PERF_G=1 bash "$C/campaign.sh" steady my s-my-r1 0.5 80000 60 210 40
bash "$C/campaign.sh" steady my s-my-r2 0.5 80000 60 210
bash "$C/campaign.sh" steady my s-my-r3 0.5 80000 60 210
bash "$C/campaign.sh" purgelogs my
echo PHASE_MY_DONE
