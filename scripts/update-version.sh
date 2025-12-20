#!/bin/bash
set -e

# update-version.sh - Updates version across all packages in the ReifyDB monorepo
# Usage: ./update-version.sh <new_version>

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if version argument is provided
if [ $# -ne 1 ]; then
    echo -e "${RED}Error: Version argument required${NC}"
    echo "Usage: $0 <new_version>"
    echo "Example: $0 1.2.3"
    exit 1
fi

NEW_VERSION=$1
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Validate version format (semantic versioning)
if ! [[ $NEW_VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo -e "${RED}Error: Invalid version format. Must be semantic versioning (x.y.z)${NC}"
    exit 1
fi

echo -e "${BLUE}Updating ReifyDB monorepo to version ${NEW_VERSION}${NC}"
echo ""

# Function to update a file with sed (cross-platform)
update_file() {
    local file=$1
    local pattern=$2

    sed -i "$pattern" "$file"
}

# Step 1: Update Rust workspace version in root Cargo.toml
echo -e "${YELLOW}[1/4] Updating Rust workspace version...${NC}"
cd "$ROOT_DIR"

# Update the workspace.package.version in root Cargo.toml
if grep -q '^\[workspace\.package\]' Cargo.toml; then
    # Find the [workspace.package] section and update the version
    update_file Cargo.toml "/^\[workspace\.package\]/,/^\[/ s/^version = \".*\"/version = \"$NEW_VERSION\"/"
    echo -e "${GREEN}✓ Updated workspace version in Cargo.toml${NC}"
else
    echo -e "${RED}Warning: [workspace.package] section not found in Cargo.toml${NC}"
fi

# Update workspace dependencies versions (all internal reifydb-* packages)
if grep -q '^\[workspace\.dependencies\]' Cargo.toml; then
    update_file Cargo.toml "/^\[workspace\.dependencies\]/,/^\[/ s/^\(reifydb[a-z-]* = { version = \)\"[^\"]*\"/\1\"$NEW_VERSION\"/"
    echo -e "${GREEN}✓ Updated workspace dependencies versions in Cargo.toml${NC}"
else
    echo -e "${RED}Warning: [workspace.dependencies] section not found in Cargo.toml${NC}"
fi
echo ""

# Step 2: Update Cargo.lock to reflect new version
echo -e "${YELLOW}[2/4] Updating Cargo.lock...${NC}"
cargo update --workspace
echo -e "${GREEN}✓ Updated Cargo.lock${NC}"

# Step 3: Update TypeScript package versions
echo -e "${YELLOW}[3/4] Updating TypeScript package versions...${NC}"

# TypeScript packages to update
TS_PACKAGES=(
    "pkg/typescript/core/package.json"
    "pkg/typescript/client/package.json"
    "pkg/typescript/react/package.json"
    "crates/sub-admin/webapp/package.json"
)

for package_file in "${TS_PACKAGES[@]}"; do
    if [ -f "$ROOT_DIR/$package_file" ]; then
        # Update version field only (keep workspace dependencies as workspace:*)
        jq --arg version "$NEW_VERSION" '.version = $version' "$ROOT_DIR/$package_file" > "$ROOT_DIR/$package_file.tmp" && \
        mv "$ROOT_DIR/$package_file.tmp" "$ROOT_DIR/$package_file"

        echo -e "${GREEN}✓ Updated $package_file${NC}"
    else
        echo -e "${YELLOW}  Warning: $package_file not found${NC}"
    fi
done

# Update root TypeScript package.json if it exists
if [ -f "$ROOT_DIR/pkg/typescript/package.json" ]; then
    jq --arg version "$NEW_VERSION" '.version = $version' "$ROOT_DIR/pkg/typescript/package.json" > "$ROOT_DIR/pkg/typescript/package.json.tmp" && \
    mv "$ROOT_DIR/pkg/typescript/package.json.tmp" "$ROOT_DIR/pkg/typescript/package.json"
    echo -e "${GREEN}✓ Updated pkg/typescript/package.json${NC}"
fi

# Step 4: Update pnpm-lock.yaml
echo -e "${YELLOW}[4/4] Updating pnpm-lock.yaml...${NC}"
cd "$ROOT_DIR/pkg/typescript"
if command -v pnpm &> /dev/null; then
    pnpm install --lockfile-only
    echo -e "${GREEN}✓ Updated pnpm-lock.yaml${NC}"
else
    echo -e "${YELLOW}  Warning: pnpm not found, skipping pnpm-lock.yaml update${NC}"
fi
cd "$ROOT_DIR"

# Create version marker file
echo "$NEW_VERSION" > "$ROOT_DIR/.version"
echo -e "${GREEN}✓ Created .version marker file${NC}"

echo ""
echo -e "${GREEN}=====================================${NC}"
echo -e "${GREEN}Successfully updated all packages to version ${NEW_VERSION}${NC}"
echo -e "${GREEN}=====================================${NC}"
echo ""
echo -e "${YELLOW}Summary of changes:${NC}"
echo "  • Rust workspace: $NEW_VERSION"
echo "  • TypeScript packages: $NEW_VERSION"
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo "  1. Review the changes with: git diff"
echo "  2. Commit changes: git add -A && git commit -m \"chore: bump version to $NEW_VERSION\""
echo "  3. Create tag: git tag v$NEW_VERSION"
echo "  4. Publish packages: make release"