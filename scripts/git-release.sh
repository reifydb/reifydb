#!/bin/bash
set -e

# git-release.sh - Creates git commit and tag for a release
# Usage: ./git-release.sh <version> [--push] [--sign]

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if version argument is provided
if [ $# -lt 1 ]; then
    echo -e "${RED}Error: Version argument required${NC}"
    echo "Usage: $0 <version> [--push] [--sign]"
    echo "Example: $0 1.2.3"
    exit 1
fi

VERSION=$1
PUSH_TO_REMOTE=0
SIGN_TAG=0
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Parse additional arguments
for arg in "${@:2}"; do
    case $arg in
        --push)
            PUSH_TO_REMOTE=1
            ;;
        --sign)
            SIGN_TAG=1
            ;;
    esac
done

cd "$ROOT_DIR"

echo -e "${BLUE}Creating git release for version ${VERSION}${NC}"
echo ""

# Check if we have uncommitted changes (should be the version updates)
if [ -z "$(git status --porcelain)" ]; then
    echo -e "${YELLOW}Warning: No changes to commit. Version might already be updated.${NC}"
    echo -e "${YELLOW}Checking if tag already exists...${NC}"

    if git rev-parse "v${VERSION}" >/dev/null 2>&1; then
        echo -e "${GREEN}✓ Tag v${VERSION} already exists${NC}"
        CURRENT_TAG_COMMIT=$(git rev-parse "v${VERSION}")
        CURRENT_HEAD=$(git rev-parse HEAD)

        if [ "$CURRENT_TAG_COMMIT" != "$CURRENT_HEAD" ]; then
            echo -e "${YELLOW}Warning: Tag exists but points to different commit${NC}"
            echo -e "${YELLOW}  Tag commit:  $CURRENT_TAG_COMMIT${NC}"
            echo -e "${YELLOW}  HEAD commit: $CURRENT_HEAD${NC}"
            exit 1
        fi

        echo -e "${GREEN}Tag already points to current HEAD. Skipping git operations.${NC}"
        exit 0
    else
        echo -e "${YELLOW}No tag found. Proceeding to create tag without commit.${NC}"
        CREATE_COMMIT=0
    fi
else
    CREATE_COMMIT=1
fi

# Step 1: Stage all changes
if [ $CREATE_COMMIT -eq 1 ]; then
    echo -e "${YELLOW}[1/3] Staging changes...${NC}"

    # Show what will be committed
    echo -e "${BLUE}Changes to be committed:${NC}"
    git status --short

    # Stage all changes
    git add -A

    # Double-check what's staged
    STAGED_FILES=$(git diff --cached --name-only | wc -l)
    echo -e "${GREEN}✓ Staged ${STAGED_FILES} files${NC}"
fi

# Step 2: Create commit
if [ $CREATE_COMMIT -eq 1 ]; then
    echo -e "${YELLOW}[2/3] Creating release commit...${NC}"

    COMMIT_MSG="chore: release v${VERSION}

This commit updates all package versions to ${VERSION} across:
- Rust workspace crates
- TypeScript packages (@reifydb/core, @reifydb/client, @reifydb/react)

All packages share the same semantic version as part of the monorepo release strategy."

    git commit -m "$COMMIT_MSG"

    echo -e "${GREEN}✓ Created release commit${NC}"

    # Show commit info
    echo -e "${BLUE}Commit details:${NC}"
    git log -1 --oneline
else
    echo -e "${YELLOW}[1/2] Skipping commit creation (no changes)${NC}"
fi

# Step 3: Create tag
if [ $CREATE_COMMIT -eq 1 ]; then
    echo -e "${YELLOW}[3/3] Creating git tag...${NC}"
else
    echo -e "${YELLOW}[2/2] Creating git tag...${NC}"
fi

# Check if tag already exists
if git rev-parse "v${VERSION}" >/dev/null 2>&1; then
    echo -e "${YELLOW}Warning: Tag v${VERSION} already exists${NC}"
    echo -e "${YELLOW}Deleting existing local tag...${NC}"
    git tag -d "v${VERSION}"
fi

# Create annotated tag
TAG_MSG="Release v${VERSION}

ReifyDB monorepo release ${VERSION}

This release includes:
- Rust crates (published to crates.io)
- TypeScript packages (published to npm)

All packages are versioned together as ${VERSION}."

if [ $SIGN_TAG -eq 1 ]; then
    echo -e "${BLUE}Creating signed tag...${NC}"
    git tag -s "v${VERSION}" -m "$TAG_MSG"
else
    echo -e "${BLUE}Creating annotated tag...${NC}"
    git tag -a "v${VERSION}" -m "$TAG_MSG"
fi

echo -e "${GREEN}✓ Created tag v${VERSION}${NC}"

# Show tag info
echo -e "${BLUE}Tag details:${NC}"
git show "v${VERSION}" --no-patch

# Step 4: Push to remote (optional)
if [ $PUSH_TO_REMOTE -eq 1 ]; then
    echo ""
    echo -e "${YELLOW}[4/4] Pushing to remote...${NC}"

    # Determine remote and branch
    REMOTE="origin"
    BRANCH=$(git branch --show-current)

    echo -e "${BLUE}Pushing to ${REMOTE}/${BRANCH}...${NC}"

    # Push commit
    if [ $CREATE_COMMIT -eq 1 ]; then
        git push "$REMOTE" "$BRANCH"
        echo -e "${GREEN}✓ Pushed commit to ${REMOTE}/${BRANCH}${NC}"
    fi

    # Push tag
    git push "$REMOTE" "v${VERSION}"
    echo -e "${GREEN}✓ Pushed tag v${VERSION} to ${REMOTE}${NC}"
else
    echo ""
    echo -e "${YELLOW}Note: Changes have not been pushed to remote.${NC}"
    echo -e "${YELLOW}To push manually, run:${NC}"
    if [ $CREATE_COMMIT -eq 1 ]; then
        echo -e "${BLUE}  git push origin $(git branch --show-current)${NC}"
    fi
    echo -e "${BLUE}  git push origin v${VERSION}${NC}"
fi

# Summary
echo ""
echo -e "${GREEN}=====================================${NC}"
echo -e "${GREEN}Git release operations completed!${NC}"
echo -e "${GREEN}=====================================${NC}"
echo ""
echo -e "${GREEN}Summary:${NC}"
if [ $CREATE_COMMIT -eq 1 ]; then
    echo -e "${GREEN}  ✓ Created release commit${NC}"
fi
echo -e "${GREEN}  ✓ Created tag v${VERSION}${NC}"
if [ $PUSH_TO_REMOTE -eq 1 ]; then
    echo -e "${GREEN}  ✓ Pushed to remote${NC}"
else
    echo -e "${YELLOW}  ⚠ Not pushed to remote (use --push flag)${NC}"
fi

exit 0