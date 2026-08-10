#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
processing_env=${FT_PROCESSING_ENV:-$script_dir/processing.env}
install_root=${FT_CAMPUS_ROOT:-/persistent/ft-shadow-data-plane}

for command_name in apptainer sbatch flock ssh; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "missing command: $command_name" >&2
        exit 1
    fi
done

for path in "$processing_env" "$install_root/central.yaml"; do
    if [ ! -r "$path" ]; then
        echo "missing or unreadable file: $path" >&2
        exit 1
    fi
done

set -a
. "$processing_env"
set +a
: "${FT_DATA_SIF:?FT_DATA_SIF is required}"
: "${FT_RAW_ROOT:?FT_RAW_ROOT is required}"
: "${FT_DERIVED_ROOT:?FT_DERIVED_ROOT is required}"
: "${FT_COLLECTOR:?FT_COLLECTOR is required}"
: "${FT_L2_CONCURRENCY:?FT_L2_CONCURRENCY is required}"

if [ ! -r "$FT_DATA_SIF" ]; then
    echo "missing or unreadable SIF: $FT_DATA_SIF" >&2
    exit 1
fi
for path in "$FT_RAW_ROOT" "$FT_DERIVED_ROOT"; do
    if [ ! -d "$path" ] || [ ! -w "$path" ]; then
        echo "directory must exist and be writable: $path" >&2
        exit 1
    fi
done

case "$FT_L2_CONCURRENCY" in
    *[!0-9]*|0|'')
        echo "FT_L2_CONCURRENCY must be a positive integer" >&2
        exit 1
        ;;
esac

apptainer exec "$FT_DATA_SIF" ft-data-pull --help >/dev/null
apptainer exec "$FT_DATA_SIF" ft-data-process --help >/dev/null
sbatch --version
echo "campus-107 deployment checks passed"
