#!/bin/bash
#
# Copyright (C) 2020-2024 Confluent, Inc.
#

set -o errexit -o nounset -o pipefail
echo "${PRIVATE_KEY}"
echo -e "${PRIVATE_KEY}" | gpg --import --batch --no-tty
cat <<GPG_AGENT_CFG > ~/.gnupg/gpg-agent.conf
use-agent
pinentry-mode loopback
allow-preset-passphrase
GPG_AGENT_CFG

gpg-connect-agent reloadagent /bye
KEY_GRIP=$(gpg --with-keygrip -K --with-colons | grep '^grp' | head -1 | cut -d: -f10)
"$(gpgconf --list-dirs libexecdir)"/gpg-preset-passphrase --preset "${KEY_GRIP}" <<< "${PASSPHRASE}"

gpg --with-keygrip -k --with-colons | grep '^fpr' | head -1 | cut -d: -f10 > keygrip.txt
echo "Determined KEY_ID: $(cat keygrip.txt)"
rm -f keygrip.txt

