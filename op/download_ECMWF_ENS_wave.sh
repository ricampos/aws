#!/usr/bin/env bash

# bash download_ECMWF_ENS_wave.sh 00 1 /home/ec2-user/SageMaker/work/data/ECMWF/wave/00 2

set -euo pipefail

usage() {
  cat <<-USAGE
Usage: $0 CYCLE [PAST_DAYS] [TARGET_DIR] [MAX_JOBS]
  CYCLE       00 or 12 forecast cycle
  PAST_DAYS   days before today to download (default: 1)
  TARGET_DIR  output directory (default: /scratch4/AOML/aoml-phod/Ricardo.Campos/data/archives/ECMWF)
  MAX_JOBS    how many concurrent member downloads (default: 1)
Example:
  $0 00 1 /scratch4/AOML/aoml-phod/Ricardo.Campos/data/archives/ECMWF 2
USAGE
}

if [[ ${1:-""} == "" || ${1:-"?"} =~ ^(-h|--help)$ ]]; then
  usage
  exit 0
fi

HCYCLE="$1"
if [[ ! "$HCYCLE" =~ ^(00|12)$ ]]; then
  echo "Error: cycle must be 00 or 12." >&2
  usage
  exit 2
fi

pa="${2:-1}"
if ! [[ "$pa" =~ ^[0-9]+$ ]]; then
  echo "Error: PAST_DAYS must be a non-negative integer." >&2
  usage
  exit 2
fi

TARGET_DIR="${3:-/scratch4/AOML/aoml-phod/Ricardo.Campos/data/archives/ECMWF}"
MAX_JOBS="${4:-1}"
if ! [[ "$MAX_JOBS" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: MAX_JOBS must be a positive integer." >&2
  usage
  exit 2
fi

CYCLE_DIR="$TARGET_DIR"

YEAR=$(date --date="-${pa} day" +%Y)
MONTH=$(date --date="-${pa} day" +%m)
DAY=$(date --date="-${pa} day" +%d)
DATE="${YEAR}$(printf "%02d" "$MONTH")$(printf "%02d" "$DAY")"

LOG_DIR="${TARGET_DIR}/logs"
mkdir -p "$TARGET_DIR" "$LOG_DIR"

echo "Start ECMWF ENS wave download: date=$DATE cycle=$HCYCLE dir=$TARGET_DIR"

CHOUR="$HCYCLE"
CHOUR=$(printf "%02d" "$CHOUR")

LOG_FILE="${LOG_DIR}/download_ECMWF_ENS_wave_${DATE}${CHOUR}.log"
exec > >(tee -a "$LOG_FILE") 2>&1

# Check required commands
for cmd in python3 cdo ncks; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Error: required command '$cmd' not found." >&2
    exit 3
  fi
done

# Domain/compression settings
wparam_wave="swh/mwd/pp1d/mwp"
latmin=-82.
latmax=89.
dp=2

# Lead times: 0-144 by 3, then 150-360 by 6
FLEADS=( $(seq -f "%g" 0 3 144) $(seq -f "%g" 150 6 360) )
STEP=$(IFS=/; echo "${FLEADS[*]}")

# ensemble members 00-30 for oper, 01-50 for pert
ENSMEM=( $(seq -f "%02g" 0 1 50) )

WORKDIR="${CYCLE_DIR}/work_${DATE}${CHOUR}"
mkdir -p "$WORKDIR"
cd "$WORKDIR"

cleanup() {
  echo "Cleaning temporary workdir: $WORKDIR"
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

run_download() {
  local ensemble="$1"
  local member_num=$((10#$ensemble))
  local etype="pf"
  local number_line=""
  if [[ "$member_num" -eq 0 ]]; then
    etype="cf"
    number_line=""
  else
    number_line="        \"number\": \"${member_num}\","  # 1..50 for pert members
  fi

  local base="ECMWF_ENS_wave_${DATE}${CHOUR}.${ensemble}"
  local grib2="${CYCLE_DIR}/${base}.grib2"
  local nc="${CYCLE_DIR}/${base}.nc"

  if [[ -f "$nc" ]]; then
    size=$(stat -c%s "$nc")
    if (( size >= 210000000 )); then
      echo "Skipping existing $nc ($size bytes)"
      return 0
    fi
    echo "Removing stale small $nc ($size bytes)"
    rm -f "$nc"
  fi

  python3 - <<PYTHON
from ecmwf.opendata import Client
client = Client()
client.retrieve({
    "class": "od",
    "date": "${DATE}",
    "time": "${CHOUR}",
    "stream": "waef",
    "type": "${etype}",
${number_line}
    "step": "${STEP}",
    "levtype": "sfc",
    "param": "${wparam_wave}",
    "target": "${grib2}"
})
PYTHON

  if [[ ! -f "$grib2" ]]; then
    echo "Error: download failed for member ${ensemble}" >&2
    return 1
  fi

  echo "Converting $grib2 to NetCDF"
  cdo -f nc4 copy "$grib2" "${CYCLE_DIR}/${base}.saux1.nc"
  ncks -4 -L 1 -d lat,${latmin},${latmax} "${CYCLE_DIR}/${base}.saux1.nc" "${CYCLE_DIR}/${base}.saux2.nc"
  ncks --ppc default=.${dp} "${CYCLE_DIR}/${base}.saux2.nc" "$nc"

  rm -f "${CYCLE_DIR}/${base}.saux1.nc" "${CYCLE_DIR}/${base}.saux2.nc" "$grib2"
  chmod 660 "$nc"
  echo "Saved $nc"
}

run_download_with_retries() {
  local member="$1"
  local attempts=0
  local max_attempts=3
  while (( attempts < max_attempts )); do
    ((attempts++))
    run_download "$member" && return 0
    echo "Retry $attempts/$max_attempts for member $member"
    sleep 2
  done
  echo "Failed after $max_attempts attempts: member $member" >&2
  return 1
}

pids=()
for member in "${ENSMEM[@]}"; do
  while (( $(jobs -rp | wc -l) >= MAX_JOBS )); do
    sleep 0.2
  done

  run_download_with_retries "$member" &
  pids+=("$!")
done

status=0
for pid in "${pids[@]}"; do
  if ! wait "$pid"; then
    status=1
  fi
done

if [[ "$status" -ne 0 ]]; then
  echo "One or more members failed." >&2
  exit 1
fi

echo "ECMWF ENS wave download complete: ${DATE}${CHOUR}."
