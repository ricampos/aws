#!/usr/bin/env bash

# input: cycle hour (00,06,12,18), days before, output path, number of parallel proc
# bash download_ECMWF_ENS_wave.sh 00 1 /home/ec2-user/SageMaker/work/data/ECMWF/wave/00 8

# Set PATH and activate environment FIRST, before strict mode
export PATH=/home/ec2-user/SageMaker/conda/tools/bin:$PATH
source /home/ec2-user/.bashrc
. /home/ec2-user/SageMaker/bashrc

#  enable strict mode
set -euo pipefail

usage() {
  cat <<-USAGE
Usage: $0 CYCLE [PAST_DAYS] [TARGET_DIR] [MAX_JOBS]
  CYCLE       00, 06, 12, or 18 forecast cycle
  PAST_DAYS   days before today to download (default: 1)
  TARGET_DIR  output directory (default: /home/sagemaker-user/work/data/ECMWF)
  MAX_JOBS    parallel conversion jobs (default: 8)
Example:
  $0 00 1 /home/sagemaker-user/work/data/ECMWF
USAGE
}

if [[ ${1:-""} == "" || ${1:-"?"} =~ ^(-h|--help)$ ]]; then
  usage
  exit 0
fi

HCYCLE="$1"
if [[ ! "$HCYCLE" =~ ^(00|06|12|18)$ ]]; then
  echo "Error: cycle must be 00, 06, 12, or 18." >&2
  exit 2
fi

pa="${2:-1}"
TARGET_DIR="${3:-/home/sagemaker-user/work/data/ECMWF}"

YEAR=$(date --date="-${pa} day" +%Y)
MONTH=$(date --date="-${pa} day" +%m)
DAY=$(date --date="-${pa} day" +%d)
DATE="${YEAR}${MONTH}${DAY}"

# Create date-cycle subdirectory
DOWNLOAD_DIR="${TARGET_DIR}/${DATE}${HCYCLE}"
mkdir -p "$DOWNLOAD_DIR"

echo "Downloading ECMWF wave ensemble from S3: date=$DATE cycle=${HCYCLE}z"

S3_PATH="s3://ecmwf-forecasts/${DATE}/${HCYCLE}z/ifs/0p25/waef/"

# Download all GRIB2 files (exclude -ep files)
aws s3 sync "$S3_PATH" "$DOWNLOAD_DIR/" \
  --region eu-central-1 \
  --no-sign-request \
  --exclude "*" \
  --include "*-ef.grib2"

echo "Download complete. Files saved to: $DOWNLOAD_DIR"
num_files=$(ls -1 "$DOWNLOAD_DIR"/*.grib2 2>/dev/null | wc -l)
echo "$num_files files downloaded"

# Standardize filenames: 20260331000000-6h -> 2026033100-006h
echo "Standardizing filenames..."
for f in "$DOWNLOAD_DIR"/*-ef.grib2; do
  [[ -f "$f" ]] || continue
  base=$(basename "$f")
  # Extract: YYYYMMDDHH0000-Xh-waef-ef.grib2 -> YYYYMMDDHH-XXXh-waef-ef.grib2
  if [[ "$base" =~ ^([0-9]{10})0000-([0-9]+)h-(.*)$ ]]; then
    new_name="${BASH_REMATCH[1]}-$(printf "%03d" ${BASH_REMATCH[2]})h-${BASH_REMATCH[3]}"
    [[ "$base" != "$new_name" ]] && mv "$DOWNLOAD_DIR/$base" "$DOWNLOAD_DIR/$new_name"
  fi
done

# Verify downloads
echo "Verifying downloads..."
sleep 2

# Check required commands for conversion
missing=0
for cmd in cdo ncks; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Warning: '$cmd' not found." >&2
    missing=1
  fi
done

if [[ $missing -eq 1 ]]; then
  echo "Skipping conversion due to missing tools." >&2
  exit 0
fi

# Domain/compression settings
latmin=-82.
latmax=89.
dp=2
MAX_JOBS="${4:-8}"

echo "Starting conversion to NetCDF and compression (parallel jobs: $MAX_JOBS)..."

process_grib() {
  local grib2="$1"
  local DOWNLOAD_DIR="$2"
  local latmin="$3"
  local latmax="$4"
  local dp="$5"
  
  local base=$(basename "$grib2" .grib2)
  local nc="${DOWNLOAD_DIR}/${base}.nc"
  
  [[ -f "$nc" ]] && return 0
  
  if ! cdo -s -f nc4 -selname,swh,mwd,pp1d,mwp -sellonlatbox,-180,180,${latmin},${latmax} "$grib2" "${nc}.tmp" 2>/dev/null; then
    rm -f "${nc}.tmp"
    echo "Failed: $(basename $grib2)" >&2
    return 1
  fi
  
  if ! ncks -4 -L 1 --ppc default=.${dp} "${nc}.tmp" "$nc" 2>/dev/null; then
    rm -f "${nc}.tmp" "$nc"
    echo "Failed: $(basename $grib2)" >&2
    return 1
  fi
  
  rm -f "${nc}.tmp"
  rm -f "$grib2"
  chmod 660 "$nc"
  echo "Created $(basename $nc)"
}

export -f process_grib
export DOWNLOAD_DIR latmin latmax dp

echo "Converting $(find "$DOWNLOAD_DIR" -name "*-ef.grib2" | wc -l) files..."
find "$DOWNLOAD_DIR" -name "*-ef.grib2" | xargs -P "$MAX_JOBS" -I {} bash -c 'process_grib "$@"' _ {} "$DOWNLOAD_DIR" "$latmin" "$latmax" "$dp"

# Summary
total_grib=$(find "$DOWNLOAD_DIR" -name "*-ef.grib2" | wc -l)
total_nc=$(find "$DOWNLOAD_DIR" -name "*-ef.nc" | wc -l)
echo "Conversion complete: $total_nc/$total_grib files converted for ${DATE}${HCYCLE}."

