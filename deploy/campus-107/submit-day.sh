#!/bin/sh
set -eu

if [ "$#" -ne 2 ]; then
    echo "usage: $0 YYYY-MM-DD /shared/path/to/symbols.txt" >&2
    exit 2
fi

utc_date=$1
symbols_file=$2
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
processing_env=${FT_PROCESSING_ENV:-$script_dir/processing.env}

normalized_date=$(date -u -d "$utc_date" +%F 2>/dev/null) || {
    echo "invalid UTC date: $utc_date" >&2
    exit 2
}
if [ "$normalized_date" != "$utc_date" ]; then
    echo "UTC date must use YYYY-MM-DD: $utc_date" >&2
    exit 2
fi
if [ ! -r "$processing_env" ]; then
    echo "missing processing environment: $processing_env" >&2
    exit 1
fi
if [ ! -r "$symbols_file" ]; then
    echo "symbols file is not readable: $symbols_file" >&2
    exit 1
fi

set -a
. "$processing_env"
set +a
: "${FT_APPTAINER:?FT_APPTAINER is required}"
: "${FT_DATA_IMAGE:?FT_DATA_IMAGE is required}"
: "${FT_RAW_ROOT:?FT_RAW_ROOT is required}"
: "${FT_DERIVED_ROOT:?FT_DERIVED_ROOT is required}"
: "${FT_COLLECTOR:?FT_COLLECTOR is required}"
: "${FT_L2_CONCURRENCY:?FT_L2_CONCURRENCY is required}"

previous_date=$(date -u -d "$utc_date -1 day" +%F)
previous_raw="$FT_RAW_ROOT/collector=$FT_COLLECTOR/day-manifests/date=$previous_date/SEALED.json"
previous_processed="$FT_DERIVED_ROOT/quality/collector=$FT_COLLECTOR/date=$previous_date/_PROCESSED.json"
if [ -e "$previous_raw" ] && [ ! -s "$previous_processed" ]; then
    echo "previous UTC day must be processed first: $previous_date" >&2
    exit 1
fi

case "$FT_L2_CONCURRENCY" in
    *[!0-9]*|0|'')
        echo "FT_L2_CONCURRENCY must be a positive integer" >&2
        exit 1
        ;;
esac

if ! awk 'NF != 1 || $1 !~ /^[A-Z0-9]{1,30}$/ || seen[$1]++ { exit 1 } END { if (NR == 0) exit 1 }' \
    "$symbols_file"
then
    echo "symbols must be unique uppercase Binance symbols, one per line, with no blanks" >&2
    exit 1
fi

symbols_file=$(readlink -f "$symbols_file")
symbol_count=$(awk 'END { print NR }' "$symbols_file")
array_max=$((symbol_count - 1))
symbols=$(awk 'BEGIN { separator = "" } { printf "%s%s", separator, $1; separator = "," }' \
    "$symbols_file")

export FT_UTC_DATE=$utc_date
export FT_SYMBOLS_FILE=$symbols_file
export FT_SYMBOLS=$symbols

normalize_result=$(sbatch --parsable "$script_dir/slurm/normalize.sbatch")
normalize_job=${normalize_result%%;*}
l2_result=$(sbatch \
    --parsable \
    --dependency="afterok:$normalize_job" \
    --array="0-$array_max%$FT_L2_CONCURRENCY" \
    "$script_dir/slurm/l2-array.sbatch")
l2_job=${l2_result%%;*}
finalize_result=$(sbatch \
    --parsable \
    --dependency="afterok:$l2_job" \
    "$script_dir/slurm/finalize.sbatch")
finalize_job=${finalize_result%%;*}

echo "normalize_job=$normalize_job"
echo "l2_job=$l2_job"
echo "finalize_job=$finalize_job"
