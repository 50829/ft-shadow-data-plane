#!/bin/sh
set -eu

if [ "$(id -u)" -ne 0 ]; then
    echo "run this script as root" >&2
    exit 1
fi
if [ "$#" -ne 1 ] || [ ! -r "$1" ]; then
    echo "usage: $0 /path/to/campus-rsync-key.pub" >&2
    exit 2
fi

public_key=$(
    awk '
        {
            for (field = 1; field < NF; field++) {
                if ($field ~ /^ssh-(ed25519|rsa)$/) {
                    print $field " " $(field + 1)
                }
            }
        }
    ' "$1"
)
if [ -z "$public_key" ] || [ "$(printf '%s\n' "$public_key" | wc -l)" -ne 1 ]; then
    echo "public key must contain exactly one SSH public key" >&2
    exit 1
fi

legacy_config=/etc/ssh/sshd_config.d/ft-data-puller.conf
if [ -f "$legacy_config" ]; then
    if ! grep -Eq '^Match[[:space:]]+User[[:space:]]+data-puller[[:space:]]*$' \
        "$legacy_config" \
        || ! grep -Eq '^[[:space:]]+ForceCommand[[:space:]]+internal-sftp[[:space:]]*$' \
            "$legacy_config"
    then
        echo "refusing to disable unrecognized legacy SSH config: $legacy_config" >&2
        exit 1
    fi
    mv "$legacy_config" "$legacy_config.disabled-v0.2"
fi

install -d -o root -g root -m 755 /etc/ssh/authorized_keys
gateway=/opt/ft-shadow-data-plane/deploy/vultr/rsync_gateway.py
if [ ! -x "$gateway" ]; then
    echo "missing restricted rsync gateway: $gateway" >&2
    exit 1
fi
key_options="restrict,command=\"$gateway\""
printf '%s %s\n' "$key_options" "$public_key" \
    > /etc/ssh/authorized_keys/data-puller
chown root:data-puller /etc/ssh/authorized_keys/data-puller
chmod 640 /etc/ssh/authorized_keys/data-puller
if ! runuser -u data-puller -- test -r /etc/ssh/authorized_keys/data-puller; then
    echo "data-puller cannot read its AuthorizedKeysFile" >&2
    exit 1
fi

install -d -o root -g root -m 755 /etc/ssh/sshd_config.d
install -o root -g root -m 600 /dev/null /etc/ssh/sshd_config.d/60-ft-data-rsync.conf
printf '%s\n' \
    'Match User data-puller' \
    '    AuthorizedKeysFile /etc/ssh/authorized_keys/%u' \
    '    AuthenticationMethods publickey' \
    '    PasswordAuthentication no' \
    '    KbdInteractiveAuthentication no' \
    '    PermitTTY no' \
    '    AllowTcpForwarding no' \
    '    X11Forwarding no' \
    >> /etc/ssh/sshd_config.d/60-ft-data-rsync.conf

sshd -t
effective=$(sshd -T -C user=data-puller,host=localhost,addr=127.0.0.1)
force_command=$(printf '%s\n' "$effective" | awk '$1 == "forcecommand" { print $2 }')
chroot_directory=$(printf '%s\n' "$effective" | awk '$1 == "chrootdirectory" { print $2 }')
if [ "$force_command" != none ] || [ "$chroot_directory" != none ]; then
    echo "data-puller still has conflicting ForceCommand or ChrootDirectory" >&2
    exit 1
fi
systemctl reload ssh 2>/dev/null || systemctl reload sshd
echo "restricted rsync access configured for data-puller"
