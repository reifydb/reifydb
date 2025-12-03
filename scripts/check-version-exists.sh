#!/bin/bash
set -e

# check-version-exists.sh - Checks if a version already exists in git or package registries
# Usage: ./check-version-exists.sh <version>

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if version argument is provided
if [ $# -ne 1 ]; then
    echo -e "${RED}Error: Version argument required${NC}"
    echo "Usage: $0 <version>"
    echo "Example: $0 1.2.3"
    exit 1
fi

VERSION=$1
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Track if any version exists
VERSION_EXISTS=0
ERRORS=""

echo -e "${BLUE}Checking if version ${VERSION} already exists...${NC}"
echo ""

# Step 1: Check git tags
echo -e "${YELLOW}[1/3] Checking git tags...${NC}"
if git tag | grep -q "^v${VERSION}$"; then
    echo -e "${RED}✗ Git tag v${VERSION} already exists${NC}"
    VERSION_EXISTS=1
else
    echo -e "${GREEN}✓ Git tag v${VERSION} does not exist${NC}"
fi

# Step 2: Check crates.io for Rust packages
echo -e "${YELLOW}[2/3] Checking crates.io...${NC}"

# List of crates to check (only the ones we plan to publish)
RUST_CRATES=(
    "reifydb"
    "reifydb-auth"
    "reifydb-catalog"
    "reifydb-cdc"
    "reifydb-client"
    "reifydb-compression"
    "reifydb-core"
    "reifydb-engine"
    "reifydb-flow-operator-abi"
    "reifydb-flow-operator-sdk"
    "reifydb-hash"
    "reifydb-network"
    "reifydb-rql"
    "reifydb-store-column"
    "reifydb-store-transaction"
    "reifydb-storage"
    "reifydb-sub-admin"
    "reifydb-sub-api"
    "reifydb-sub-flow"
    "reifydb-sub-tracing"
    "reifydb-sub-server"
    "reifydb-sub-worker"
    "reifydb-testing"
    "reifydb-transaction"
    "reifydb-type"
)

CRATES_CHECKED=0
CRATES_FOUND=0

for crate in "${RUST_CRATES[@]}"; do
    # Use cargo search to check if crate exists with specific version
    # Note: This only works if the crate has been published before
    if command -v cargo &> /dev/null; then
        # Check if the crate exists on crates.io at all
        SEARCH_RESULT=$(cargo search "${crate}" --limit 1 2>/dev/null || true)
        if echo "$SEARCH_RESULT" | grep -q "^${crate} "; then
            # Crate exists, check version
            PUBLISHED_VERSION=$(echo "$SEARCH_RESULT" | grep "^${crate} " | sed -n 's/.*= "\([^"]*\)".*/\1/p')
            if [ "$PUBLISHED_VERSION" == "$VERSION" ]; then
                echo -e "${RED}  ✗ ${crate} version ${VERSION} already exists on crates.io${NC}"
                VERSION_EXISTS=1
                CRATES_FOUND=$((CRATES_FOUND + 1))
            fi
            CRATES_CHECKED=$((CRATES_CHECKED + 1))
        fi
    fi
done

if [ $CRATES_FOUND -eq 0 ]; then
    if [ $CRATES_CHECKED -eq 0 ]; then
        echo -e "${YELLOW}  No crates found on crates.io (packages not yet published)${NC}"
    else
        echo -e "${GREEN}  ✓ No Rust crates with version ${VERSION} found on crates.io${NC}"
    fi
fi

# Step 3: Check npm registry for TypeScript packages
echo -e "${YELLOW}[3/3] Checking npm registry...${NC}"

NPM_PACKAGES=(
    "@reifydb/core"
    "@reifydb/client"
    "@reifydb/react"
)

NPM_FOUND=0

for package in "${NPM_PACKAGES[@]}"; do
    if command -v npm &> /dev/null; then
        # Check if specific version exists on npm
        # npm view returns error if package@version doesn't exist
        if npm view "${package}@${VERSION}" version &>/dev/null; then
            echo -e "${RED}  ✗ ${package} version ${VERSION} already exists on npm${NC}"
            VERSION_EXISTS=1
            NPM_FOUND=$((NPM_FOUND + 1))
        fi
    else
        echo -e "${YELLOW}  Warning: npm not found, cannot check npm registry${NC}"
        break
    fi
done

if [ $NPM_FOUND -eq 0 ] && command -v npm &> /dev/null; then
    echo -e "${GREEN}  ✓ No TypeScript packages with version ${VERSION} found on npm${NC}"
fi

# Final result
echo ""
if [ $VERSION_EXISTS -eq 1 ]; then
    # Check if this is a resumable state (partial publish)
    if [ $NPM_FOUND -gt 0 ] && [ $CRATES_FOUND -eq 0 ]; then
        echo -e "${YELLOW}=====================================${NC}"
        echo -e "${YELLOW}PARTIAL RELEASE DETECTED${NC}"
        echo -e "${YELLOW}=====================================${NC}"
        echo ""
        echo -e "${BLUE}NPM packages are published, but Rust crates are not.${NC}"
        echo -e "${BLUE}You can resume this release with:${NC}"
        echo -e "${GREEN}  make release VERSION=${VERSION} FORCE=1 SKIP_TESTS=1${NC}"
        echo ""
        echo -e "${RED}Or choose a different version number.${NC}"
        exit 1
    elif [ $TAG_EXISTS -eq 1 ] && [ $CRATES_FOUND -eq 0 ] && [ $NPM_FOUND -eq 0 ]; then
        echo -e "${YELLOW}=====================================${NC}"
        echo -e "${YELLOW}GIT TAG EXISTS BUT PACKAGES NOT PUBLISHED${NC}"
        echo -e "${YELLOW}=====================================${NC}"
        echo ""
        echo -e "${BLUE}Git tag exists, but packages are not published.${NC}"
        echo -e "${BLUE}You can resume this release with:${NC}"
        echo -e "${GREEN}  make release VERSION=${VERSION} FORCE=1${NC}"
        echo ""
        echo -e "${RED}Or delete the tag and choose a different version:${NC}"
        echo -e "${YELLOW}  git tag -d v${VERSION}${NC}"
        exit 1
    else
        echo -e "${RED}=====================================${NC}"
        echo -e "${RED}ERROR: Version ${VERSION} already exists!${NC}"
        echo -e "${RED}=====================================${NC}"
        echo ""
        echo -e "${YELLOW}You cannot release a version that already exists.${NC}"
        echo -e "${YELLOW}Please choose a different version number.${NC}"
        echo ""
        echo -e "${BLUE}Or force the release with:${NC}"
        echo -e "${GREEN}  make release VERSION=${VERSION} FORCE=1${NC}"
        exit 1
    fi
else
    echo -e "${GREEN}=====================================${NC}"
    echo -e "${GREEN}Version ${VERSION} is available for release${NC}"
    echo -e "${GREEN}=====================================${NC}"
    exit 0
fi