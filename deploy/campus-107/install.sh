#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
    echo "usage: $0 /path/to/ft-shadow-data-plane.sif" >&2
    exit 2
fi

release_sif=$1
if [ ! -r "$release_sif" ]; then
    echo "release SIF is not readable: $release_sif" >&2
    exit 1
fi

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
install_root=${FT_CAMPUS_ROOT:-/persistent/ft-shadow-data-plane}
deploy_root=$install_root/deploy/campus-107

install -d -m 750 \
    "$install_root" \
    "$install_root/raw" \
    "$install_root/derived" \
    "$install_root/symbols" \
    "$deploy_root/slurm"
release_hash=$(sha256sum "$release_sif" | cut -d' ' -f1)
release_name=ft-shadow-data-plane-$release_hash.sif
release_target=$install_root/$release_name
if [ -e "$release_target" ]; then
    installed_hash=$(sha256sum "$release_target" | cut -d' ' -f1)
    if [ "$installed_hash" != "$release_hash" ]; then
        echo "installed release has an unexpected hash: $release_target" >&2
        exit 1
    fi
else
    install -m 555 "$release_sif" "$release_target"
fi
ln -sfn "$release_name" "$install_root/ft-shadow-data-plane.sif"
install -m 555 "$script_dir/submit-day.sh" "$deploy_root/submit-day.sh"
install -m 555 "$script_dir/verify.sh" "$deploy_root/verify.sh"
install -m 444 "$script_dir/README.md" "$deploy_root/README.md"
install -m 444 "$script_dir/central.yaml.example" "$deploy_root/central.yaml.example"
install -m 444 "$script_dir/processing.env.example" "$deploy_root/processing.env.example"
install -m 444 "$script_dir/crontab.example" "$deploy_root/crontab.example"
install -m 444 "$script_dir/slurm/normalize.sbatch" "$deploy_root/slurm/normalize.sbatch"
install -m 444 "$script_dir/slurm/l2-array.sbatch" "$deploy_root/slurm/l2-array.sbatch"
install -m 444 "$script_dir/slurm/finalize.sbatch" "$deploy_root/slurm/finalize.sbatch"

if [ ! -e "$install_root/central.yaml" ]; then
    install -m 600 "$script_dir/central.yaml.example" "$install_root/central.yaml"
fi
if [ ! -e "$deploy_root/processing.env" ]; then
    install -m 600 "$script_dir/processing.env.example" "$deploy_root/processing.env"
fi

echo "installed campus release $release_hash under $install_root"
echo "next: edit $install_root/central.yaml and $deploy_root/processing.env"
echo "then: run $deploy_root/verify.sh before installing cron"
