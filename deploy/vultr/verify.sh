#!/bin/sh
set -eu

if [ "$(id -u)" -ne 0 ]; then
    echo "run this verifier as root" >&2
    exit 1
fi

deploy_root=/opt/ft-shadow-data-plane/deploy/vultr
config_root=/etc/ft-shadow-data-plane

for command_name in docker systemctl sshd; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "missing command: $command_name" >&2
        exit 1
    fi
done

for path in \
    "$deploy_root/compose.yaml" \
    "$config_root/edge.env" \
    "$config_root/edge.yaml" \
    "$config_root/alert.env"
do
    if [ ! -r "$path" ]; then
        echo "missing or unreadable file: $path" >&2
        exit 1
    fi
done

set -a
. "$config_root/edge.env"
set +a
: "${EDGE_IMAGE:?EDGE_IMAGE is required}"
: "${EDGE_DATA_ROOT:?EDGE_DATA_ROOT is required}"
: "${EDGE_CONFIG:?EDGE_CONFIG is required}"

case "$EDGE_IMAGE" in
    *@sha256:*) ;;
    *)
        echo "EDGE_IMAGE must use an immutable sha256 digest" >&2
        exit 1
        ;;
esac
case "$EDGE_IMAGE" in
    *REPLACE_WITH_RELEASE_DIGEST*)
        echo "replace the placeholder in EDGE_IMAGE" >&2
        exit 1
        ;;
esac

for relative_path in ready writing control control/acks control/universe/inbox; do
    if [ ! -d "$EDGE_DATA_ROOT/$relative_path" ]; then
        echo "missing data directory: $EDGE_DATA_ROOT/$relative_path" >&2
        exit 1
    fi
done

docker compose -f "$deploy_root/compose.yaml" config --quiet
sshd -t
systemctl is-active --quiet ft-shadow-data-plane.service
docker compose -f "$deploy_root/compose.yaml" ps
echo "Vultr deployment checks passed"
