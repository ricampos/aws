#!/bin/bash
# Two input arguments
# date
CTIME="$1"
# cycle 00,06,12,18
HCYCLE="$2"
# destination path
DIRW="$3"
# S3 bucket
BUCKET=s3://noaa-gefs-pds
# ensemble members
ensblm="`seq -f "%02g" 0 1 30`"
# Forecast lead time (hours) to download
fleads="`seq -f "%03g" 0 6 384`"

cd ${DIRW}
for h in $fleads; do
  echo " ======== GEFS Forecast, AWS S3 direct: ${CTIME} ${HCYCLE}Z $h ========"
  for e in $ensblm; do
    echo $e
    FILE=$DIRW/gefs.wave.${CTIME}.${e}.global.0p25.f$(printf "%03.f" $h).grib2

    # Skip if file exists and is large enough
    if [ -f "$FILE" ]; then
      TAM=$(du -sb "$FILE" | awk '{ print $1 }')
      if [ "$TAM" -ge 10000000 ]; then
        echo "File $FILE already exists and is large enough. Skipping."
        continue
      fi
    fi

    # Build S3 source path (c00 for control, p01-p30 for perturbeds)
    if [ ${e} == "00" ]; then
      S3FILE=${BUCKET}/gefs.${CTIME}/${HCYCLE}/wave/gridded/gefs.wave.t${HCYCLE}z.c${e}.global.0p25.f$(printf "%03.f" $h).grib2
    else
      S3FILE=${BUCKET}/gefs.${CTIME}/${HCYCLE}/wave/gridded/gefs.wave.t${HCYCLE}z.p${e}.global.0p25.f$(printf "%03.f" $h).grib2
    fi

    echo "Copying: $S3FILE"
    aws s3 cp $S3FILE $FILE --no-sign-request

    # Check if file was downloaded and has minimum size
    if [ -f "$FILE" ]; then
      TAM=$(du -sb "$FILE" | awk '{ print $1 }')
      if [ "$TAM" -lt 1200000 ]; then
        echo "Warning: file $FILE is too small ($TAM bytes), may be corrupted."
      fi
    else
      echo "Error: failed to copy $S3FILE"
    fi

  done
done

echo " Done ${CTIME}."

