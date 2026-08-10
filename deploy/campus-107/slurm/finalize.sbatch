#!/bin/sh
#SBATCH --job-name=ft-finalize
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=00:30:00
#SBATCH --mail-type=FAIL

set -eu
: "${FT_DATA_SIF:?}"
: "${FT_RAW_ROOT:?}"
: "${FT_DERIVED_ROOT:?}"
: "${FT_COLLECTOR:?}"
: "${FT_UTC_DATE:?}"
: "${FT_SYMBOLS:?}"

apptainer exec "$FT_DATA_SIF" ft-data-process finalize \
  --raw-root "$FT_RAW_ROOT" \
  --derived-root "$FT_DERIVED_ROOT" \
  --collector "$FT_COLLECTOR" \
  --date "$FT_UTC_DATE" \
  --symbols "$FT_SYMBOLS"
