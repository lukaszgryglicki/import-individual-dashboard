#!/bin/bash
# NCPUS=12 DEBUG=1 DRY='' DEBUG_SQL=1 ./import.sh prod
if [ -z "$1" ]
then
  echo "$0: you need to specify env: test|prod"
  exit 1
fi
export SH_DSN=$(cat "./SH_DSN.${1}.secret")
if [ -z "$SH_DSN" ]
then
  echo "$0: missing SH credentials secret for ${1} env"
  exit 2
fi
# ./import user_identities_202201061433.csv user_affiliations_202201061525.csv
./import ui.csv ua.csv
