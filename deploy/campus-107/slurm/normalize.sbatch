#!/bin/sh
#SBATCH --job-name=ft-normalize
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --time=04:00:00
#SBATCH --mail-type=FAIL

set -eu
: "${FT_APPTAINER:?}"
: "${FT_DATA_IMAGE:?}"
: "${FT_RAW_ROOT:?}"
: "${FT_DERIVED_ROOT:?}"
: "${FT_COLLECTOR:?}"
: "${FT_UTC_DATE:?}"

"$FT_APPTAINER" exec --writable "$FT_DATA_IMAGE" ft-data-process normalize \
  --raw-root "$FT_RAW_ROOT" \
  --derived-root "$FT_DERIVED_ROOT" \
  --collector "$FT_COLLECTOR" \
  --date "$FT_UTC_DATE"
