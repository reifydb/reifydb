# Publishing Guide for @reifydb Packages

This guide covers publishing `@reifydb/core` and `@reifydb/client` packages to npm.

## Prerequisites

1. **npm account**: Ensure you're logged in to npm
   ```bash
   pnpm login
   ```

2. **Access rights**: Verify you have publish access to the @reifydb scope
   ```bash
   pnpm org ls @reifydb
   ```

## Publishing Process

### Build and Test

Ensure all packages build and tests pass:

```bash
# Build all packages
pnpm build

# Run all tests
pnpm test
```

###  Publish Packages

**Important**: Publish `@reifydb/core` first, then `@reifydb/client` (due to dependency)

#### Option A: Publish individually

```bash
# Publish core package first
pnpm publish:core

# Then publish client package
pnpm publish:client
```

#### Option B: Publish all at once

```bash
# This will publish core first, then client
pnpm publish:all
```
