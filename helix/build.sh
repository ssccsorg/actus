#!/usr/bin/env bash
# helix/build.sh — One-stop: clone→patch→build→run
#
# Usage:
#   ./build.sh                          # full flow (clone→patch→build→run)
#   ./build.sh --build-only             # clone→patch→build only
#   ./build.sh --force                  # force rebuild even if cached
#   ./build.sh --release                # release build (slow, ~15min)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"     # apps//helix
HELIX_DIR="$SCRIPT_DIR"
REPO_DIR="$HELIX_DIR/subtree"                    # cloned repo
PATCHES_DIR="$HELIX_DIR/patch"                   # local patches
BIN_DIR="$HELIX_DIR/.bin"
BIN="$BIN_DIR/helix-zed-headless-arm64"
FORCE=false
RELEASE=false
BUILD_ONLY=false

while [ $# -gt 0 ]; do
    case "$1" in
        --force) FORCE=true ;;
        --release) RELEASE=true ;;
        --build-only) BUILD_ONLY=true ;;
        --help|-h)
            echo "Usage: $0 [--force] [--release] [--build-only]"
            exit 0 ;;
        *) echo "Unknown: $1 (see --help)"; exit 1 ;;
    esac
    shift
done

# ── Step 1: Clone Helix fork if not present ───────────────────────────

if [ ! -d "$REPO_DIR/.git" ]; then
    echo "==> Cloning helixml/zed into $REPO_DIR..."
    git clone --depth 1 https://github.com/helixml/zed "$REPO_DIR"
else
    echo "==> Helix fork exists at $REPO_DIR"
fi

# ── Step 2: Apply patches ─────────────────────────────────────────────

NEEDS_BUILD=false
COMMITTED_HASH="$(git -C "$REPO_DIR" rev-parse HEAD 2>/dev/null || echo "")"
CACHE_FILE="$BIN_DIR/.build_hash"

for patch in "$PATCHES_DIR"/*.patch; do
    [ -f "$patch" ] || continue
    name=$(basename "$patch")
    if git -C "$REPO_DIR" apply --check "$patch" 2>/dev/null; then
        echo "==> Applying patch: $name"
        git -C "$REPO_DIR" apply "$patch"
        NEEDS_BUILD=true
    else
        echo "==> Patch already applied: $name"
    fi
done

# ── Step 3: Check cache ───────────────────────────────────────────────

if [ -f "$BIN" ] && [ -f "$CACHE_FILE" ] && [ "$FORCE" = false ]; then
    CACHED_HASH="$(cat "$CACHE_FILE" 2>/dev/null || echo "")"
    if [ "$CACHED_HASH" = "$COMMITTED_HASH" ] && [ "$NEEDS_BUILD" = false ]; then
        echo "==> Binary is up-to-date. Skip build."
        if [ "$BUILD_ONLY" = false ]; then
            echo "==> Starting ..."
            python3 "$SCRIPT_DIR/../runner.py"
        fi
        exit 0
    fi
fi

# ── Step 4: Build ─────────────────────────────────────────────────────

mkdir -p "$BIN_DIR"
cd "$REPO_DIR"

if [ "$RELEASE" = true ]; then
    echo "==> Building release (this may take 10-15 min)..."
    cargo build -p zed --features external_websocket_sync --release
    SRC="$REPO_DIR/target/release/zed"
else
    echo "==> Building debug..."
    cargo build -p zed --features external_websocket_sync
    SRC="$REPO_DIR/target/debug/zed"
fi

if [ ! -f "$SRC" ]; then
    echo "ERROR: Build failed — $SRC not created"
    exit 1
fi

cp "$SRC" "$BIN"
echo "$COMMITTED_HASH" > "$CACHE_FILE"
echo "==> Deployed: $BIN ($(ls -lh "$BIN" | awk '{print $5}'))"

# ── Step 5: Run ───────────────────────────────────────────────────────

if [ "$BUILD_ONLY" = true ]; then
    echo "==> Build complete."
    exit 0
fi

echo "==> Starting ..."
python3 "$SCRIPT_DIR/../runner.py"
