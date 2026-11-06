#!/usr/bin/env bash
# Cross-compile only FreeBSD x64. Native Linux preparation runs on the host.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
if [[ $# -ne 0 ]]; then echo 'Usage: scripts/prepare-freebsd.sh' >&2; exit 2; fi
if [[ "$(uname -s)/$(uname -m)" != Linux/x86_64 ]]; then
  echo 'The Docker cross builder requires a Linux x64 host.' >&2; exit 2
fi
for command in docker cargo rustc rustup; do command -v "$command" >/dev/null; done
toolchain="$(rustc --print sysroot)"
if [[ ! -d "$toolchain/lib/rustlib/x86_64-unknown-freebsd/lib" ]]; then
  echo 'Install the matching standard library first: rustup target add x86_64-unknown-freebsd' >&2
  exit 1
fi
project="$(pwd -P)"
workspace="$(dirname "$project")"
cache="${ORBIT_FREEBSD_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/claviron-js-freebsd}"
mkdir -p "$cache/cargo" "$cache/target"
image="claviron-js-freebsd-builder:14.4"
registry="${CARGO_HOME:-$HOME/.cargo}/registry"
# Fetch through the operator's Cargo configuration; credentials never enter Docker.
cargo fetch --locked --target x86_64-unknown-freebsd
docker build --network "${ORBIT_FREEBSD_BUILD_NETWORK:-default}" --tag "$image" scripts/freebsd
# No host networking, privileged access, live SHM, credentials or exposed ports.
# Only the source tree is read; prebuilds/ is the sole writable source mount.
mkdir -p prebuilds
docker run --rm --network none --user "$(id -u):$(id -g)" --cap-drop ALL \
  --security-opt no-new-privileges --cpus "${CARGO_BUILD_JOBS:-4}" --memory 8g \
  --mount "type=bind,src=$project,dst=/workspace/claviron-js,readonly" \
  --mount "type=bind,src=$workspace/claviron,dst=/workspace/claviron,readonly" \
  --mount "type=bind,src=$workspace/orbitive,dst=/workspace/orbitive,readonly" \
  --mount "type=bind,src=$project/prebuilds,dst=/workspace/claviron-js/prebuilds" \
  --mount "type=bind,src=$toolchain,dst=/opt/rust,readonly" \
  --mount "type=bind,src=$cache,dst=/cache" \
  --mount "type=bind,src=$registry,dst=/cache/cargo/registry,readonly" \
  --env CARGO_NET_OFFLINE=true \
  --env CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-4}" "$image"
