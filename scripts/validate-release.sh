#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 ReifyDB
set -e

# validate-release.sh - Validates that the codebase is ready for release
# Usage: ./validate-release.sh [version]

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

VERSION=${1:-""}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Track validation status
VALIDATION_FAILED=0
WARNINGS=0

echo -e "${BLUE}Validating release readiness...${NC}"
if [ -n "$VERSION" ]; then
    echo -e "${BLUE}Target version: ${VERSION}${NC}"
fi
echo ""

# Function to check command existence
command_exists() {
    command -v "$1" &> /dev/null
}

# Step 1: Check git status
echo -e "${YELLOW}[1/7] Checking git status...${NC}"
cd "$ROOT_DIR"

# Check for uncommitted changes
if [ -n "$(git status --porcelain)" ]; then
    echo -e "${RED}✗ There are uncommitted changes:${NC}"
    git status --short
    VALIDATION_FAILED=1
else
    echo -e "${GREEN}✓ Working directory is clean${NC}"
fi

# Check current branch
CURRENT_BRANCH=$(git branch --show-current)
if [ "$CURRENT_BRANCH" != "main" ] && [ "$CURRENT_BRANCH" != "master" ]; then
    echo -e "${YELLOW}⚠ Warning: Not on main branch (current: ${CURRENT_BRANCH})${NC}"
    WARNINGS=$((WARNINGS + 1))
else
    echo -e "${GREEN}✓ On main branch${NC}"
fi

# Check if up to date with remote
git fetch origin >/dev/null 2>&1
LOCAL_COMMIT=$(git rev-parse HEAD)
REMOTE_COMMIT=$(git rev-parse origin/main 2>/dev/null || git rev-parse origin/master 2>/dev/null || echo "")

if [ -n "$REMOTE_COMMIT" ] && [ "$LOCAL_COMMIT" != "$REMOTE_COMMIT" ]; then
    echo -e "${YELLOW}⚠ Warning: Local branch is not up to date with remote${NC}"
    WARNINGS=$((WARNINGS + 1))
else
    echo -e "${GREEN}✓ Branch is up to date with remote${NC}"
fi

# Step 2: Validate version format
if [ -n "$VERSION" ]; then
    echo -e "${YELLOW}[2/7] Validating version format...${NC}"
    if ! [[ $VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo -e "${RED}✗ Invalid version format. Must be semantic versioning (x.y.z)${NC}"
        VALIDATION_FAILED=1
    else
        echo -e "${GREEN}✓ Version format is valid${NC}"
    fi
else
    echo -e "${YELLOW}[2/7] Skipping version format validation (no version specified)${NC}"
fi

# Step 3: Check Rust workspace
echo -e "${YELLOW}[3/7] Checking Rust workspace...${NC}"

# Check if cargo is available
if command_exists cargo; then
    # Check that Cargo.toml exists
    if [ ! -f "$ROOT_DIR/Cargo.toml" ]; then
        echo -e "${RED}✗ Cargo.toml not found${NC}"
        VALIDATION_FAILED=1
    else
        # Check workspace configuration
        if ! grep -q '^\[workspace\]' "$ROOT_DIR/Cargo.toml"; then
            echo -e "${RED}✗ No workspace configuration found in Cargo.toml${NC}"
            VALIDATION_FAILED=1
        else
            echo -e "${GREEN}✓ Workspace configuration found${NC}"
        fi

        # Check that all crates use workspace version
        CRATE_DIRS=$(find "$ROOT_DIR/crates" "$ROOT_DIR/bin" "$ROOT_DIR/pkg/rust" -name "Cargo.toml" -type f 2>/dev/null)
        NON_WORKSPACE_CRATES=0

        for crate_toml in $CRATE_DIRS; do
            if grep -q '^version = ' "$crate_toml" | grep -v 'workspace = true'; then
                echo -e "${YELLOW}  ⚠ Warning: $(basename $(dirname "$crate_toml")) does not use workspace version${NC}"
                NON_WORKSPACE_CRATES=$((NON_WORKSPACE_CRATES + 1))
            fi
        done

        if [ $NON_WORKSPACE_CRATES -eq 0 ]; then
            echo -e "${GREEN}✓ All crates use workspace version${NC}"
        else
            echo -e "${YELLOW}⚠ ${NON_WORKSPACE_CRATES} crates don't use workspace version${NC}"
            WARNINGS=$((WARNINGS + 1))
        fi

        # Run cargo check
        echo -e "${BLUE}  Running cargo check...${NC}"
        if cargo check --workspace --all-features >/dev/null 2>&1; then
            echo -e "${GREEN}✓ Cargo check passed${NC}"
        else
            echo -e "${RED}✗ Cargo check failed${NC}"
            VALIDATION_FAILED=1
        fi
    fi
else
    echo -e "${RED}✗ Cargo not found${NC}"
    VALIDATION_FAILED=1
fi

# Step 4: Check TypeScript packages
echo -e "${YELLOW}[4/7] Checking TypeScript packages...${NC}"

if [ -d "$ROOT_DIR/pkg/typescript" ]; then
    cd "$ROOT_DIR/pkg/typescript"

    # Check if pnpm is available
    if command_exists pnpm; then
        # Check package.json files exist
        MISSING_PACKAGES=0
        for pkg in core client react; do
            if [ ! -f "$ROOT_DIR/pkg/typescript/$pkg/package.json" ]; then
                echo -e "${RED}✗ Missing package.json for $pkg${NC}"
                MISSING_PACKAGES=$((MISSING_PACKAGES + 1))
            fi
        done

        if [ $MISSING_PACKAGES -eq 0 ]; then
            echo -e "${GREEN}✓ All TypeScript packages found${NC}"
        else
            VALIDATION_FAILED=1
        fi

        # Check if packages can build
        echo -e "${BLUE}  Running TypeScript build...${NC}"
        if pnpm run build >/dev/null 2>&1; then
            echo -e "${GREEN}✓ TypeScript packages build successfully${NC}"
        else
            echo -e "${YELLOW}⚠ Warning: TypeScript build failed${NC}"
            WARNINGS=$((WARNINGS + 1))
        fi
    else
        echo -e "${YELLOW}⚠ Warning: pnpm not found, cannot validate TypeScript packages${NC}"
        WARNINGS=$((WARNINGS + 1))
    fi
else
    echo -e "${YELLOW}⚠ Warning: TypeScript packages directory not found${NC}"
    WARNINGS=$((WARNINGS + 1))
fi

cd "$ROOT_DIR"

# Step 5: Run tests (optional, can be slow)
echo -e "${YELLOW}[5/7] Running tests...${NC}"

# Check if we should skip tests
if [ "${SKIP_TESTS}" == "1" ]; then
    echo -e "${YELLOW}  Skipping tests (SKIP_TESTS=1)${NC}"
else
    # Run Rust tests
    if command_exists cargo; then
        echo -e "${BLUE}  Running Rust tests (fast subset)...${NC}"
        if cargo test --workspace --lib --bins >/dev/null 2>&1; then
            echo -e "${GREEN}✓ Rust tests passed${NC}"
        else
            echo -e "${RED}✗ Rust tests failed${NC}"
            echo -e "${YELLOW}  Run 'cargo test' to see details${NC}"
            VALIDATION_FAILED=1
        fi
    fi

    # Run TypeScript tests
    if [ -d "$ROOT_DIR/pkg/typescript" ] && command_exists pnpm; then
        cd "$ROOT_DIR/pkg/typescript"
        echo -e "${BLUE}  Running TypeScript tests...${NC}"
        if pnpm run test >/dev/null 2>&1; then
            echo -e "${GREEN}✓ TypeScript tests passed${NC}"
        else
            echo -e "${YELLOW}⚠ Warning: TypeScript tests failed${NC}"
            WARNINGS=$((WARNINGS + 1))
        fi
        cd "$ROOT_DIR"
    fi
fi

# Step 6: Check for required tools
echo -e "${YELLOW}[6/7] Checking required tools...${NC}"

REQUIRED_TOOLS=("git" "cargo" "npm")
OPTIONAL_TOOLS=("pnpm")
MISSING_REQUIRED=0
MISSING_OPTIONAL=0

for tool in "${REQUIRED_TOOLS[@]}"; do
    if command_exists "$tool"; then
        echo -e "${GREEN}✓ $tool is available${NC}"
    else
        echo -e "${RED}✗ $tool is missing (required)${NC}"
        MISSING_REQUIRED=$((MISSING_REQUIRED + 1))
    fi
done

for tool in "${OPTIONAL_TOOLS[@]}"; do
    if ! command_exists "$tool"; then
        echo -e "${YELLOW}⚠ $tool is missing (optional)${NC}"
        MISSING_OPTIONAL=$((MISSING_OPTIONAL + 1))
    fi
done

if [ $MISSING_REQUIRED -gt 0 ]; then
    VALIDATION_FAILED=1
fi

if [ $MISSING_OPTIONAL -gt 0 ]; then
    WARNINGS=$((WARNINGS + 1))
fi

# Step 7: Check version consistency
echo -e "${YELLOW}[7/7] Checking version consistency...${NC}"

# Get version from Cargo.toml
CARGO_VERSION=$(grep -E '^version = ' "$ROOT_DIR/Cargo.toml" | head -n1 | sed 's/version = "\(.*\)"/\1/')
echo -e "${BLUE}  Cargo workspace version: ${CARGO_VERSION}${NC}"

# Check TypeScript versions
VERSION_MISMATCH=0
for pkg in core client react; do
    if [ -f "$ROOT_DIR/pkg/typescript/$pkg/package.json" ]; then
        PKG_VERSION=$(grep '"version"' "$ROOT_DIR/pkg/typescript/$pkg/package.json" | head -n1 | sed 's/.*"version": "\(.*\)".*/\1/')
        if [ "$PKG_VERSION" != "$CARGO_VERSION" ]; then
            echo -e "${YELLOW}  ⚠ TypeScript $pkg version mismatch: ${PKG_VERSION}${NC}"
            VERSION_MISMATCH=1
        fi
    fi
done

if [ $VERSION_MISMATCH -eq 0 ]; then
    echo -e "${GREEN}✓ All package versions are consistent${NC}"
else
    echo -e "${YELLOW}⚠ Version mismatch detected${NC}"
    echo -e "${YELLOW}  Run 'make set-version VERSION=${CARGO_VERSION}' to sync versions${NC}"
    WARNINGS=$((WARNINGS + 1))
fi

# Final summary
echo ""
echo -e "${BLUE}=====================================${NC}"

if [ $VALIDATION_FAILED -eq 1 ]; then
    echo -e "${RED}VALIDATION FAILED${NC}"
    echo -e "${RED}=====================================${NC}"
    echo ""
    echo -e "${RED}There are critical issues that must be resolved before release.${NC}"
    echo -e "${RED}Please fix the errors above and run validation again.${NC}"

    if [ $WARNINGS -gt 0 ]; then
        echo ""
        echo -e "${YELLOW}Additionally, there are ${WARNINGS} warning(s) that should be reviewed.${NC}"
    fi

    exit 1
elif [ $WARNINGS -gt 0 ]; then
    echo -e "${YELLOW}VALIDATION PASSED WITH WARNINGS${NC}"
    echo -e "${YELLOW}=====================================${NC}"
    echo ""
    echo -e "${YELLOW}There are ${WARNINGS} warning(s) that should be reviewed.${NC}"
    echo -e "${GREEN}The release can proceed, but consider addressing the warnings.${NC}"
    exit 0
else
    echo -e "${GREEN}VALIDATION PASSED${NC}"
    echo -e "${GREEN}=====================================${NC}"
    echo ""
    echo -e "${GREEN}All checks passed! The codebase is ready for release.${NC}"
    exit 0
fi