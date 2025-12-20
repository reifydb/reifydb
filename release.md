# ReifyDB Release Process

This document describes the release process for the ReifyDB monorepo. All packages (Rust crates and TypeScript packages)
are versioned together using semantic versioning.

## Overview

ReifyDB uses a unified versioning strategy where all packages share the same version number. This ensures consistency
across the entire ecosystem and simplifies dependency management for users.

## Prerequisites

Before releasing, ensure you have:

1. **Git** configured with push access to the repository
2. **Cargo** with crates.io publishing credentials (`cargo login`)
3. **cargo-workspaces** for automated workspace publishing (`cargo install cargo-workspaces`)
4. **npm** with registry credentials (`npm login`)
5. **pnpm** installed for TypeScript package management

## Quick Release Commands

```bash
# Show current version
make show-version

# Release with auto-increment patch version (e.g., 0.0.1 → 0.0.2)
make release

# Release specific version
make release VERSION=1.0.0

# Quick releases
make release-patch   # Increment patch version (0.0.x)
make release-minor   # Increment minor version (0.x.0)
make release-major   # Increment major version (x.0.0)

# Preview what will happen (dry run)
make release-dry-run VERSION=1.0.0
```

## Detailed Release Process

The `make release` command orchestrates the entire release process:

### 1. Version Validation

- Checks if the specified version already exists (git tag or published packages)
- Prevents duplicate releases
- Validates semantic version format (x.y.z)

### 2. Pre-release Checks

- Ensures working directory is clean (no uncommitted changes)
- Verifies you're on the main branch (warning if not)
- Runs tests to ensure everything is working
- Checks that all required tools are installed

### 3. Version Update

- Updates Rust workspace version in `Cargo.toml`
- Updates all TypeScript package versions
- Ensures all internal dependencies use the new version

### 4. Git Operations

- Creates a release commit: `chore: release vX.Y.Z`
- Creates an annotated git tag: `vX.Y.Z`
- Optionally pushes to remote repository

### 5. Package Publishing

Publishing happens automatically in topological dependency order using `cargo-workspaces`:

#### Rust Crates (crates.io)

- Automatically calculates dependency order from workspace graph
- Publishes in topological order (dependencies before dependents)
- Waits 10 seconds between publishes for crates.io indexing
- Skips binary and test crates (marked with `publish = false`)
- Typical order: Core libraries → Infrastructure → Subsystems → Client → Main library

#### TypeScript Packages (npm)

1. @reifydb/core
2. @reifydb/client
3. @reifydb/react

### 6. Push to Remote

- Pushes the release commit and tag to the remote repository

## Configuration

Release behavior can be configured in `release.toml`:

```toml
[publish]
crates_io = true          # Publish to crates.io
npm_registry = true       # Publish to npm
```

Setting either value to `false` will skip publishing to that registry.

## Manual Operations

If you need to perform individual steps manually:

```bash
# Update versions only
scripts/update-version.sh 1.0.0

# Check if version exists
scripts/check-version-exists.sh 1.0.0

# Validate release readiness
scripts/validate-release.sh 1.0.0

# Create git commit and tag
scripts/git-release.sh 1.0.0 --push

# Publish packages
scripts/publish-release.sh 1.0.0
```

## Troubleshooting

### Release Fails Midway

If publishing fails partway through:

1. The script tracks what was successfully published
2. Re-run `scripts/publish-release.sh VERSION` to continue
3. Already-published packages will be skipped

### Version Already Exists

If you get an error about version existing:

1. Check git tags: `git tag | grep vX.Y.Z`
2. Check crates.io: `cargo search reifydb`
3. Check npm: `npm view @reifydb/core versions`
4. Choose a different version number

### Rollback a Release

To rollback (best effort):

```bash
# Remove git tag
git tag -d vX.Y.Z
git push origin :refs/tags/vX.Y.Z

# Note: Published packages usually cannot be unpublished
# You may need to publish a new patch version instead
```

### Test Failures

If tests fail during validation:

```bash
# Skip tests temporarily (not recommended)
SKIP_TESTS=1 make release VERSION=1.0.0

# Or fix the tests and try again
cargo test
pnpm test
```

## Version Policy

- **Major version (x.0.0)**: Breaking API changes
- **Minor version (0.x.0)**: New features, backward compatible
- **Patch version (0.0.x)**: Bug fixes, backward compatible

All packages in the monorepo share the same version to ensure compatibility.

## Security Notes

1. Never commit credentials to the repository
2. Use `cargo login` to store crates.io token securely
3. Use `npm login` for npm authentication
4. Consider using GPG signing for release tags

## Checklist Before Release

- [ ] All tests passing (`make test`)
- [ ] Documentation updated
- [ ] Breaking changes documented
- [ ] Version number chosen appropriately
- [ ] Credentials configured for all registries
- [ ] On main branch with latest changes

## Post-Release

After a successful release:

1. Verify packages are available:
    - Check crates.io: https://crates.io/crates/reifydb
    - Check npm: https://www.npmjs.com/package/@reifydb/core

2. Update any example code or documentation with new version

3. Announce the release (if applicable)

## Getting Help

If you encounter issues:

1. Check the release logs in the console output
2. Review `scripts/*.sh` for detailed implementation
3. Check `release.toml` for configuration issues
4. Open an issue in the repository if needed

## Advanced Usage

### Partial Releases

To skip certain registries:

```bash
# Skip Rust crates
scripts/publish-release.sh 1.0.0 --skip-crates

# Skip npm packages
scripts/publish-release.sh 1.0.0 --skip-npm
```

### Dry Run

Always test with dry-run first:

```bash
# Preview all operations
make release-dry-run VERSION=1.0.0

# Dry run publishing only
scripts/publish-release.sh 1.0.0 --dry-run
```

### Using cargo-workspaces Directly

You can also use cargo-workspaces directly for more control:

```bash
# List all publishable crates in dependency order
cargo workspaces list

# Publish with custom interval (note: --registry crates-io required with vendored deps)
cargo workspaces publish --from-git --publish-interval 15 --registry crates-io

# Publish to custom registry
cargo workspaces publish --from-git --registry my-registry

# See all options
cargo workspaces publish --help
```

**Important Notes:**

- Binary and test crates are automatically excluded via `publish = false` in their Cargo.toml files
- When using vendored dependencies, you **must** include `--registry crates-io` to bypass the vendor source replacement
- The publish script automatically includes this flag

### Custom Configuration

Override configuration per release:

```bash
# Don't push to remote
scripts/git-release.sh 1.0.0  # Without --push flag

# Sign this release tag
scripts/git-release.sh 1.0.0 --sign --push
```