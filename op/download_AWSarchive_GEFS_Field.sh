#!/usr/bin/env bash

# Four input arguments:
# cycle (00, 06, 12, 18)
# how many days ago: 0 (same day), 1 (yesterday) etc
# destination path
# Max number of jobs
#
# ./download_AWSarchive_GEFS_Field.sh 06 1 /home/sagemaker-user/work/data/GEFS 8

source /home/ec2-user/SageMaker/bashrc

set -euo pipefail

usage() {
  cat <<-USAGE
Usage: $0 HCYCLE [PAST_DAYS] [TARGET_DIR] [MAX_JOBS]
  HCYCLE      Forecast cycle: 00, 06, 12, or 18
  PAST_DAYS   How many days ago to download (default: 1)
  TARGET_DIR  Data directory (default: /home/sagemaker-user/work/data/GEFS)
  MAX_JOBS    Max concurrent S3 downloads (default: 8)
Example:
  $0 06 1 /home/sagemaker-user/work/data/GEFS 8
USAGE
}

if [[ ${1:-""} == "" || ${1:-"?"} =~ ^(-h|--help)$ ]]; then
  usage
  exit 0
fi

HCYCLE="$1"
if [[ ! $HCYCLE =~ ^(00|06|12|18)$ ]]; then
  echo "Error: HCYCLE must be one of 00, 06, 12, 18." >&2
  usage
  exit 2
fi

pa=${2:-1}
if ! [[ $pa =~ ^[0-9]+$ ]]; then
  echo "Error: PAST_DAYS must be a non-negative integer." >&2
  usage
  exit 2
fi

TARGET_DIR="${3:-/home/sagemaker-user/work/data/GEFS}"
MAX_JOBS="${4:-8}"
if ! [[ $MAX_JOBS =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: MAX_JOBS must be a positive integer." >&2
  usage
  exit 2
fi

BUCKET="s3://noaa-gefs-pds"
DIRS="$TARGET_DIR"

YEAR=$(date --date="-${pa} day" +%Y)
MONTH=$(date --date="-${pa} day" +%m)
DAY=$(date --date="-${pa} day" +%d)
WTIME=${YEAR}$(printf "%02d" "${MONTH}")$(printf "%02d" "${DAY}")
DIRW="${DIRS}/GEFSv12_${HCYCLE}Z_Cycle/GEFSv12Waves_${WTIME}"
mkdir -p "${DIRW}"

# Generate forecast lead and ensemble strings once.
FLEADS=( $(seq -f "%03g" 0 6 384) )
ENSBLM=( $(seq -f "%02g" 0 1 30) )

download_member() {
  local ctime="$1" hc="$2" dirw="$3" ens="$4" lead="$5"
  local file="${dirw}/gefs.wave.${ctime}.${ens}.global.0p25.f${lead}.grib2"
  local s3key

  if [[ "$ens" == "00" ]]; then
    s3key="${BUCKET}/gefs.${ctime}/${hc}/wave/gridded/gefs.wave.t${hc}z.c${ens}.global.0p25.f${lead}.grib2"
  else
    s3key="${BUCKET}/gefs.${ctime}/${hc}/wave/gridded/gefs.wave.t${hc}z.p${ens}.global.0p25.f${lead}.grib2"
  fi

  if [[ -f "$file" ]]; then
    local size=$(stat -c%s "$file")
    if [[ $size -ge 10000000 ]]; then
      return 0
    fi
    echo "Existing file is too small ($size bytes), re-downloading: $file"
  fi

  aws s3 cp "$s3key" "$file" --no-sign-request
  if [[ $? -ne 0 ]]; then
    echo "Warning: aws s3 cp failed for $s3key" >&2
    return 1
  fi

  local final_size
  final_size=$(stat -c%s "$file" 2>/dev/null || echo 0)
  if [[ $final_size -lt 1200000 ]]; then
    echo "Warning: downloaded file looks too small ($final_size bytes): $file" >&2
    return 1
  fi
  return 0
}

echo "Starting GEFS wave archive download for cycle ${HCYCLE}Z, date ${WTIME}"

for lead in "${FLEADS[@]}"; do
  echo "===== cycle ${HCYCLE}Z lead f${lead} ====="
  for ens in "${ENSBLM[@]}"; do
    while (( $(jobs -rp | wc -l) >= MAX_JOBS )); do
      sleep 0.1
    done

    download_member "${WTIME}" "${HCYCLE}" "${DIRW}" "${ens}" "${lead}" &
  done

  # Wait for current lead batch to finish to avoid too many jobs and keep predictable speed.
  wait

done

wait

echo "Download complete for ${WTIME} ${HCYCLE}Z"

# cleanup old cycles older than 16 days
GEFSMDIR="${DIRS}/GEFSv12_${HCYCLE}Z_Cycle"
CUTOFF=$(date -d "16 days ago" +%Y%m%d)
for dir in "${GEFSMDIR}"/GEFSv12Waves_*; do
  [[ -d "$dir" ]] || continue
  basename="$(basename "$dir")"
  dirdate="${basename#GEFSv12Waves_}"
  if [[ "$dirdate" =~ ^[0-9]{8}$ && "$dirdate" -lt "$CUTOFF" ]]; then
    echo "Deleting old directory: $dir"
    rm -rf "$dir"
  fi
done

echo "Cleanup finished."
