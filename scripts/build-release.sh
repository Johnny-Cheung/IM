#!/usr/bin/env bash
# Build a deployable IM release package.
#
# Usage: scripts/build-release.sh [VERSION] [OUTPUT_DIR]
#
# The source must be a clean, committed checkout of refactor/im. The script does
# not switch branches. It builds the frontend with the committed npm lockfile and
# a Linux amd64 Go binary (CGO disabled), then writes bin/im-server, web/,
# RELEASE and SHA256SUMS to a new directory outside the repository.
set -euo pipefail
umask 022

die() {
  printf 'build-release: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage: scripts/build-release.sh [VERSION] [OUTPUT_DIR]

  VERSION     release label in vX.Y or vX.Y.Z form (default: v0.7)
  OUTPUT_DIR  new output directory outside the repository
              (default: a unique scratch directory under $TMPDIR)

Requires a clean committed refactor/im checkout plus git, go, node, npm and
either sha256sum or shasum. The script never checks out or pushes anything.
EOF
}

case "${1:-}" in
  -h|--help) usage; exit 0 ;;
esac
if [ "$#" -gt 2 ]; then
  usage >&2
  exit 2
fi

VERSION="${1:-v0.7}"
if [[ ! "$VERSION" =~ ^v[0-9]+\.[0-9]+(\.[0-9]+)?$ ]]; then
  die "invalid version '$VERSION'; expected vX.Y or vX.Y.Z (for example v0.7 or v1.8.1)"
fi

INVOCATION_DIR="$PWD"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)" \
  || die "cannot resolve script directory"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd -P)" \
  || die "cannot resolve repository root"

WORK_DIR=""
OUT_DIR=""
OUT_CONFIRMED=0

cleanup() {
  rc=$?
  trap - EXIT INT TERM
  if [ -n "$WORK_DIR" ] && [ -d "$WORK_DIR" ]; then
    rm -rf "$WORK_DIR"
  fi
  if [ "$rc" -ne 0 ] && [ "$OUT_CONFIRMED" -eq 1 ] \
    && [ -n "$OUT_DIR" ] && { [ -e "$OUT_DIR" ] || [ -L "$OUT_DIR" ]; }; then
    rm -rf "$OUT_DIR"
  fi
  if [ "$rc" -ne 0 ]; then
    printf 'build-release: failed; temporary files cleaned up\n' >&2
  fi
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for tool in git go node npm; do
  command -v "$tool" >/dev/null 2>&1 \
    || die "required command not found: $tool"
done

if command -v sha256sum >/dev/null 2>&1; then
  SHA_TOOL=sha256sum
elif command -v shasum >/dev/null 2>&1; then
  SHA_TOOL=shasum
else
  die "required command not found: sha256sum or shasum"
fi

sha256_file() {
  if [ "$SHA_TOOL" = "sha256sum" ]; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

artifact_files() {
  local base="$1"
  (
    cd "$base" || exit 1
    find bin web -type f -print | LC_ALL=C sort
  )
}

write_artifact_hashes() {
  local base="$1" path digest
  artifact_files "$base" | while IFS= read -r path; do
    digest="$(sha256_file "$base/$path")" || exit 1
    [ -n "$digest" ] || exit 1
    printf 'sha256(%s)=%s\n' "$path" "$digest"
  done
}

write_sha256sums() {
  local base="$1" output="$2" path digest
  {
    artifact_files "$base"
    printf 'RELEASE\n'
  } | LC_ALL=C sort | while IFS= read -r path; do
    digest="$(sha256_file "$base/$path")" || exit 1
    [ -n "$digest" ] || exit 1
    printf '%s  %s\n' "$digest" "$path"
  done > "$output"
}

cd "$ROOT"

TOPLEVEL="$(git rev-parse --show-toplevel 2>/dev/null)" \
  || die "not a git checkout: $ROOT"
TOPLEVEL="$(cd "$TOPLEVEL" && pwd -P)"
[ "$TOPLEVEL" = "$ROOT" ] \
  || die "script must be run from the repository checkout at $ROOT"

BRANCH="$(git symbolic-ref --quiet --short HEAD 2>/dev/null)" \
  || die "HEAD is detached; check out refactor/im manually before building"
if [ "$BRANCH" != "refactor/im" ]; then
  die "current branch is '$BRANCH'; check out refactor/im manually before building"
fi

if [ -n "$(git status --porcelain --untracked-files=normal)" ]; then
  printf 'Dirty files:\n' >&2
  git status --short >&2
  die "working tree is not clean; commit all source changes before building"
fi

COMMIT="$(git rev-parse --verify HEAD)" \
  || die "cannot resolve HEAD"
COMMIT_SHORT="$(git rev-parse --short=12 HEAD)"
TIMESTAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
GO_VERSION="$(go version | awk '{print $3}')"
NODE_VERSION="$(node --version)"

if [ -n "${2:-}" ]; then
  case "$2" in
    /*) OUT_DIR="$2" ;;
    *) OUT_DIR="$INVOCATION_DIR/$2" ;;
  esac
else
  SCRATCH_BASE="${TMPDIR:-/tmp}"
  SCRATCH_BASE="${SCRATCH_BASE%/}"
  [ -n "$SCRATCH_BASE" ] || SCRATCH_BASE=/tmp
  OUT_DIR="${SCRATCH_BASE}/im-${VERSION}-${COMMIT_SHORT}-linux-amd64-${TIMESTAMP//:/-}-$$"
fi

while [ "$OUT_DIR" != "/" ] && [ "${OUT_DIR%/}" != "$OUT_DIR" ]; do
  OUT_DIR="${OUT_DIR%/}"
done
[ "$OUT_DIR" != "/" ] || die "invalid output directory: $OUT_DIR"

OUT_PARENT="$(dirname "$OUT_DIR")"
OUT_NAME="$(basename "$OUT_DIR")"
case "$OUT_NAME" in
  ""|"."|"..") die "invalid output directory: $OUT_DIR" ;;
esac
[ -d "$OUT_PARENT" ] || die "output parent directory does not exist: $OUT_PARENT"
OUT_PARENT="$(cd "$OUT_PARENT" && pwd -P)" \
  || die "cannot resolve output parent directory"
OUT_DIR="$OUT_PARENT/$OUT_NAME"
if [ -e "$OUT_DIR" ] || [ -L "$OUT_DIR" ]; then
  die "output path already exists: $OUT_DIR"
fi
if [ "$OUT_DIR" = "$ROOT" ] || [[ "$OUT_DIR" == "$ROOT"/* ]]; then
  die "output directory must be outside the repository: $OUT_DIR"
fi
SCRATCH_BASE="${TMPDIR:-/tmp}"
SCRATCH_BASE="${SCRATCH_BASE%/}"
[ -n "$SCRATCH_BASE" ] || SCRATCH_BASE=/tmp
WORK_DIR="$(mktemp -d "${SCRATCH_BASE}/im-release-build.XXXXXX")" \
  || die "cannot create a temporary build directory"
STAGE="$WORK_DIR/release"
mkdir -p "$STAGE/bin" "$STAGE/web"

printf 'Building %s from %s (%s)\n' "$VERSION" "${COMMIT_SHORT}" "$BRANCH"
printf '  output: %s\n' "$OUT_DIR"

printf '[1/3] building frontend with npm ci\n'
(
  cd "$ROOT/frontend"
  npm ci
  npm run build
)
[ -f "$ROOT/frontend/dist/index.html" ] \
  || die "frontend build did not produce dist/index.html"
cp -R "$ROOT/frontend/dist/." "$STAGE/web/"

printf '[2/3] building Linux amd64 backend (CGO disabled)\n'
(
  cd "$ROOT/backend"
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" -o "$STAGE/bin/im-server" ./cmd/server
)
[ -s "$STAGE/bin/im-server" ] || die "backend build did not produce bin/im-server"
chmod 0755 "$STAGE/bin/im-server"
if [ "$(git -C "$ROOT" rev-parse HEAD)" != "$COMMIT" ] \
  || [ -n "$(git -C "$ROOT" status --porcelain --untracked-files=normal)" ]; then
  die "source changed during build; commit the changes and rebuild"
fi

printf '[3/3] writing release metadata and checksums\n'
{
  printf 'version=%s\n' "$VERSION"
  printf 'commit=%s\n' "$COMMIT"
  printf 'commit_short=%s\n' "$COMMIT_SHORT"
  printf 'branch=%s\n' "$BRANCH"
  printf 'built_at=%s\n' "$TIMESTAMP"
  printf 'go=%s\n' "$GO_VERSION"
  printf 'node=%s\n' "$NODE_VERSION"
  printf 'platform=linux/amd64\n'
  printf 'cgo=0\n'
  printf 'source=clean\n'
  printf '\n[sha256]\n'
  write_artifact_hashes "$STAGE"
} > "$STAGE/RELEASE"

write_sha256sums "$STAGE" "$WORK_DIR/SHA256SUMS"
mv "$WORK_DIR/SHA256SUMS" "$STAGE/SHA256SUMS"

(
  cd "$STAGE"
  if [ "$SHA_TOOL" = "sha256sum" ]; then
    sha256sum -c SHA256SUMS >/dev/null
  else
    shasum -a 256 -c SHA256SUMS >/dev/null
  fi
) || die "generated SHA256SUMS did not verify"

for required in bin/im-server web/index.html RELEASE SHA256SUMS; do
  [ -e "$STAGE/$required" ] || die "release package is missing $required"
done

mkdir "$OUT_DIR" || die "cannot claim output directory: $OUT_DIR"
OUT_CONFIRMED=1
cp -R "$STAGE/." "$OUT_DIR/"

printf '\nRelease package ready: %s\n' "$OUT_DIR"
printf '  version: %s\n' "$VERSION"
printf '  commit:  %s\n' "$COMMIT"
printf '  files:   bin/im-server, web/, RELEASE, SHA256SUMS\n'
printf '  verify:  (cd %q && %s)\n' "$OUT_DIR" \
  "$([ "$SHA_TOOL" = "sha256sum" ] && printf 'sha256sum -c SHA256SUMS' || printf 'shasum -a 256 -c SHA256SUMS')"
