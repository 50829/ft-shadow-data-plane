#!/bin/sh
#SBATCH --job-name=ft-l2
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --time=04:00:00
#SBATCH --mail-type=FAIL

set -eu
: "${FT_APPTAINER:?}"
: "${FT_DATA_IMAGE:?}"
: "${FT_RAW_ROOT:?}"
: "${FT_DERIVED_ROOT:?}"
: "${FT_COLLECTOR:?}"
: "${FT_UTC_DATE:?}"
: "${FT_SYMBOLS_FILE:?}"
: "${SLURM_ARRAY_TASK_ID:?}"

symbol=$(sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" "$FT_SYMBOLS_FILE")
test -n "$symbol"

"$FT_APPTAINER" exec --writable "$FT_DATA_IMAGE" ft-data-process l2 \
  --raw-root "$FT_RAW_ROOT" \
  --derived-root "$FT_DERIVED_ROOT" \
  --collector "$FT_COLLECTOR" \
  --date "$FT_UTC_DATE" \
  --symbol "$symbol"
