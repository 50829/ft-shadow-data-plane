#!/bin/sh
set -eu

script_root=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
install_root=${FT_CAMPUS_ROOT:-$script_root}
apptainer=${FT_APPTAINER:-/public/app/apptainer/1.4.5/bin/apptainer}
image=$install_root/ft-shadow-data-plane.sandbox
config=$install_root/central.yaml

exec "$apptainer" exec --writable "$image" \
    ft-data-pull --config "$config"
