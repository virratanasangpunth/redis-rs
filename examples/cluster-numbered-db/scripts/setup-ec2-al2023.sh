#!/usr/bin/env bash
#
# Provision a fresh Amazon Linux 2023 EC2 instance to run this example:
# installs Docker, git, and the Rust toolchain.
#
# Usage (on the instance):
#   ./scripts/setup-ec2-al2023.sh
#
# After it finishes, log out and back in (or run `newgrp docker`) so your shell
# picks up the `docker` group, then verify with: docker run --rm hello-world
set -euo pipefail

echo "==> Installing docker and git ..."
sudo dnf install -y docker git

echo "==> Enabling and starting docker ..."
sudo systemctl enable --now docker
sudo usermod -aG docker "${USER}" || true

echo "==> Installing Rust toolchain ..."
if command -v cargo >/dev/null 2>&1; then
  echo "    cargo already present: $(cargo --version)"
else
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
  # shellcheck disable=SC1091
  source "${HOME}/.cargo/env"
  echo "    installed: $(cargo --version)"
fi

cat <<'EOF'

==> Done.

Next steps:
  1. Log out and back in (or run `newgrp docker`) so docker works without sudo.
  2. Bring up the cluster:   ./scripts/cluster-up.sh
  3. Run the example:        CLUSTER_NODES=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 \
                                 cargo run -p cluster-numbered-db-example
  4. Tear it down:           ./scripts/cluster-down.sh
EOF
