#!/bin/bash

#
# bash probmaps_gefs_valdt.sh /home/ec2-user/SageMaker/work/week2/sval/probmaps_gefs_internal.yaml
#
# set -euo pipefail
source /home/ec2-user/SageMaker/bashrc

# INPUT ARGUMENT
# .yaml configuration file containing paths and information for this
#   shell script as well as for the python code.
PYCYAML="$1"

# Read the YAML as a text file:
#  GEFS data path
gefspath_line=$(grep 'gefspath' "${PYCYAML}")
GEFSMDIR=$(echo "$gefspath_line" | awk -F': ' '{print $2}')
#  Python script (probability maps)
pyscript_line=$(grep 'pyscript' "${PYCYAML}")
PYSCRIPT=$(echo "$pyscript_line" | awk -F': ' '{print $2}')
#  Variable names, for the python processing (probability maps)
mvars_line=$(grep 'mvars' "${PYCYAML}")
MVARS=$(echo "$mvars_line" | awk -F': ' '{gsub(/"/, "", $2); print $2}')
#  Output path
outpath_line=$(grep 'outpath' "${PYCYAML}")
OUTPATH=$(echo "$outpath_line" | awk -F': ' '{print $2}')

pa=15 #  days into the past. pa=1 runs using yesterday's cycle
YEAR=`date --date=-$pa' day' '+%Y'`
MONTH=`date --date=-$pa' day' '+%m'`
DAY=`date --date=-$pa' day' '+%d'`
HOUR="00" # first cycle 00Z

# Check GEFv12 is complete and ready.
# If not, it waits for 5 min and then try again (max 12 hours)
FSIZE=0
TRIES=1

while [ "$FSIZE" -lt 1000000 ] && [ "$TRIES" -le 144 ]; do

  # wait 5 minutes until next try
  if [ ${TRIES} -gt 5 ]; then
    sleep 300
  fi
  # Check if the last file (member 30, lead time 384h) is complete
  test -f $GEFSMDIR/$YEAR$MONTH$DAY$HOUR/gefs.wave.$YEAR$MONTH$DAY.30.global.0p25.f384.grib2
  TE=$?
  if [ ${TE} -eq 1 ]; then
    FSIZE=0
  else
    FSIZE=$(du -sb "$GEFSMDIR/$YEAR$MONTH$DAY$HOUR/gefs.wave.$YEAR$MONTH$DAY.30.global.0p25.f384.grib2" | awk '{print $1}')
  fi

  TRIES=`expr $TRIES + 1`

done

# Module load python and activate environment when necessary.
# source /home/Ricardo.Campos/python/envs/intelpy_env/bin/activate

rm -rf $GEFSMDIR/$YEAR$MONTH$DAY$HOUR/*.idx

echo "  "
echo " PYTHON PROCESSING: GLOBAL HAZARDS OUTLOOK (VALIDATION) - PROBABILITY MAPS, $YEAR$MONTH$DAY$HOUR "
echo "  "
# loop through variables
for WW3VAR in ${MVARS[*]}; do
  # 7 14 is the time intervall (days) for week 2
  python3 ${PYSCRIPT} ${PYCYAML} $YEAR$MONTH$DAY$HOUR 7 14 ${WW3VAR}
  echo " Probability maps for ${WW3VAR} Ok." 

done

echo "  "
echo " PYTHON PROCESSING COMPLETE."

# ----
cd ${OUTPATH}
mkdir -p $YEAR$MONTH$DAY
mkdir -p $YEAR$MONTH$DAY/Hs
mkdir -p $YEAR$MONTH$DAY/WS10
mv *Hs* $YEAR$MONTH$DAY/Hs/
mv *WS10* $YEAR$MONTH$DAY/WS10/

echo "  "
echo " Done."
echo "  "

