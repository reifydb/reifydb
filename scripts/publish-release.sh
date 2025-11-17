#!/bin/bash
set -e

# publish-release.sh - Publishes all packages to their respective registries
# Usage: ./publish-release.sh <version> [--dry-run]

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if version argument is provided
if [ $# -lt 1 ]; then
    echo -e "${RED}Error: Version argument required${NC}"
    echo "Usage: $0 <version> [--dry-run]"
    echo "Example: $0 1.2.3"
    exit 1
fi

VERSION=$1
DRY_RUN=0
SKIP_CRATES=0
SKIP_NPM=0

# Parse additional arguments
for arg in "${@:2}"; do
    case $arg in
        --dry-run)
            DRY_RUN=1
            echo -e "${YELLOW}DRY RUN MODE - No packages will be published${NC}"
            ;;
        --skip-crates)
            SKIP_CRATES=1
            ;;
        --skip-npm)
            SKIP_NPM=1
            ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Track publishing status
PUBLISH_LOG="$ROOT_DIR/.publish-status-${VERSION}.log"
FAILED_PACKAGES=""

# Function to log publishing status
log_publish() {
    local package=$1
    local status=$2
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $package - $status" >> "$PUBLISH_LOG"
}

# Function to check if package was already published
was_published() {
    local package=$1
    if [ -f "$PUBLISH_LOG" ]; then
        grep -q "$package - SUCCESS" "$PUBLISH_LOG" 2>/dev/null
    else
        return 1
    fi
}

# Function to run command (respects dry-run mode)
run_cmd() {
    local cmd="$@"
    if [ $DRY_RUN -eq 1 ]; then
        echo -e "${BLUE}  [DRY RUN] Would execute: $cmd${NC}"
    else
        echo -e "${BLUE}  Executing: $cmd${NC}"
        eval "$cmd"
    fi
}

# Function to restore cargo config (called on exit)
restore_cargo_config() {
    if [ -f "$ROOT_DIR/.cargo/config.toml.disabled" ]; then
        echo -e "${BLUE}Restoring .cargo/config.toml...${NC}"
        mv "$ROOT_DIR/.cargo/config.toml.disabled" "$ROOT_DIR/.cargo/config.toml"
        echo -e "${GREEN}✓ Cargo config restored${NC}"
    fi
}

# Set up cleanup trap to always restore config
trap restore_cargo_config EXIT

echo -e "${BLUE}Publishing ReifyDB version ${VERSION}${NC}"
echo ""

# Check if config file exists and source it
if [ -f "$ROOT_DIR/release.toml" ]; then
    echo -e "${GREEN}✓ Found release.toml configuration${NC}"
    # Parse basic settings from release.toml (simplified parsing)
    if grep -q "crates_io = false" "$ROOT_DIR/release.toml"; then
        SKIP_CRATES=1
        echo -e "${YELLOW}  Skipping crates.io publishing (disabled in config)${NC}"
    fi
    if grep -q "npm_registry = false" "$ROOT_DIR/release.toml"; then
        SKIP_NPM=1
        echo -e "${YELLOW}  Skipping npm publishing (disabled in config)${NC}"
    fi
fi

# Phase 1: Publish Rust crates to crates.io
if [ $SKIP_CRATES -eq 0 ]; then
    echo -e "${YELLOW}[Phase 1/2] Publishing Rust crates to crates.io${NC}"

    # Check if cargo-workspaces is installed
    if ! command -v cargo-workspaces &> /dev/null; then
        echo -e "${RED}  Error: cargo-workspaces not installed${NC}"
        echo -e "${YELLOW}  Install with: cargo install cargo-workspaces${NC}"
        echo -e "${YELLOW}  Or temporarily use CARGO_NET_OFFLINE=false cargo install cargo-workspaces${NC}"
        exit 1
    fi

    cd "$ROOT_DIR"

    # Temporarily disable .cargo/config.toml to allow publishing to crates.io
    if [ -f "$ROOT_DIR/.cargo/config.toml" ]; then
        echo -e "${BLUE}  Temporarily disabling .cargo/config.toml for publishing...${NC}"
        mv "$ROOT_DIR/.cargo/config.toml" "$ROOT_DIR/.cargo/config.toml.disabled"
        echo -e "${GREEN}  ✓ Cargo config disabled${NC}"
    fi

    # Show publishing order
    echo -e "${BLUE}  Publishing order (automatically calculated):${NC}"
    cargo workspaces list | sed 's/^/    /'
    echo ""

    # Build cargo-workspaces command
    # --publish-as-is uses current version from Cargo.toml
    # --allow-branch release restricts publishing to release branch only
    # Note: --registry crates-io removed as it causes issues with cargo-workspaces
    CARGO_WS_ARGS="publish --publish-as-is --allow-branch release --publish-interval 10 --no-verify"

    if [ $DRY_RUN -eq 1 ]; then
        CARGO_WS_ARGS="$CARGO_WS_ARGS --dry-run"
        echo -e "${BLUE}  Running cargo-workspaces in dry-run mode...${NC}"
    fi

    # Set token if available
    if [ -n "${CRATES_TOKEN:-}" ]; then
        CARGO_WS_ARGS="$CARGO_WS_ARGS --token ${CRATES_TOKEN}"
    elif [ -n "${CARGO_REGISTRY_TOKEN:-}" ]; then
        CARGO_WS_ARGS="$CARGO_WS_ARGS --token ${CARGO_REGISTRY_TOKEN}"
    fi

    # Publish all crates using cargo-workspaces
    echo -e "${BLUE}  Publishing Rust crates...${NC}"
    if cargo workspaces $CARGO_WS_ARGS; then
        log_publish "crates:all" "SUCCESS"
        echo -e "${GREEN}  ✓ All Rust crates published successfully${NC}"
    else
        log_publish "crates:all" "FAILED"
        echo -e "${RED}  ✗ Failed to publish Rust crates${NC}"
        FAILED_PACKAGES="$FAILED_PACKAGES crates:workspace"
        # Continue to Phase 2 (NPM) even if Rust publishing fails
    fi
else
    echo -e "${YELLOW}[Phase 1/2] Skipping Rust crates (--skip-crates)${NC}"
fi

# Phase 2: Publish TypeScript packages to npm
if [ $SKIP_NPM -eq 0 ]; then
    echo ""
    echo -e "${YELLOW}[Phase 2/2] Publishing TypeScript packages to npm${NC}"

    # Order matters: core -> client -> react
    NPM_PACKAGES=("core" "client" "react")

    for package in "${NPM_PACKAGES[@]}"; do
        PACKAGE_DIR="$ROOT_DIR/pkg/typescript/$package"
        PACKAGE_NAME="@reifydb/$package"

        echo -e "${BLUE}  Publishing $PACKAGE_NAME...${NC}"

        # Check if already published
        if was_published "npm:$PACKAGE_NAME"; then
            echo -e "${GREEN}    ✓ Already published (from previous run)${NC}"
            continue
        fi

        if [ ! -d "$PACKAGE_DIR" ]; then
            echo -e "${YELLOW}    ⚠ Skipping $PACKAGE_NAME (directory not found)${NC}"
            continue
        fi

        cd "$PACKAGE_DIR"

        # Build the package first
        if [ -f "package.json" ] && grep -q '"build"' package.json; then
            echo -e "${BLUE}    Building package...${NC}"
            if ! run_cmd "pnpm run build"; then
                echo -e "${YELLOW}    ⚠ Build failed, attempting to publish anyway${NC}"
            fi
        fi

        # Publish to npm (without any tag, just semantic version)
        if run_cmd "npm publish --access public"; then
            log_publish "npm:$PACKAGE_NAME" "SUCCESS"
            echo -e "${GREEN}    ✓ Published $PACKAGE_NAME${NC}"
        else
            log_publish "npm:$PACKAGE_NAME" "FAILED"
            echo -e "${RED}    ✗ Failed to publish $PACKAGE_NAME${NC}"
            FAILED_PACKAGES="$FAILED_PACKAGES npm:$PACKAGE_NAME"
        fi

        cd "$ROOT_DIR"
    done
else
    echo -e "${YELLOW}[Phase 2/2] Skipping npm packages (--skip-npm)${NC}"
fi

# Summary
echo ""
echo -e "${BLUE}=====================================${NC}"

if [ -n "$FAILED_PACKAGES" ]; then
    echo -e "${YELLOW}PUBLISHING COMPLETED WITH ERRORS${NC}"
    echo -e "${YELLOW}=====================================${NC}"
    echo ""
    echo -e "${YELLOW}The following packages failed to publish:${NC}"
    for pkg in $FAILED_PACKAGES; do
        echo -e "${RED}  ✗ $pkg${NC}"
    done
    echo ""
    echo -e "${YELLOW}You can retry publishing by running this script again.${NC}"
    echo -e "${YELLOW}Successfully published packages will be skipped.${NC}"
    echo -e "${YELLOW}Status log: $PUBLISH_LOG${NC}"
    exit 1
else
    if [ $DRY_RUN -eq 1 ]; then
        echo -e "${GREEN}DRY RUN COMPLETED SUCCESSFULLY${NC}"
        echo -e "${GREEN}=====================================${NC}"
        echo ""
        echo -e "${GREEN}All packages would be published successfully.${NC}"
        echo -e "${BLUE}Run without --dry-run to actually publish.${NC}"
    else
        echo -e "${GREEN}ALL PACKAGES PUBLISHED SUCCESSFULLY${NC}"
        echo -e "${GREEN}=====================================${NC}"
        echo ""
        echo -e "${GREEN}Version ${VERSION} has been published to:${NC}"
        if [ $SKIP_CRATES -eq 0 ]; then
            echo -e "${GREEN}  ✓ crates.io (Rust packages)${NC}"
        fi
        if [ $SKIP_NPM -eq 0 ]; then
            echo -e "${GREEN}  ✓ npm registry (TypeScript packages)${NC}"
        fi

        # Clean up status log on success
        rm -f "$PUBLISH_LOG"
    fi
    exit 0
fi