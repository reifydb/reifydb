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

### 1. Update Versions

From the TypeScript workspace root (`pkg/typescript/`):

```bash
# For patch release (0.0.1 -> 0.0.2)
pnpm version:patch

# For minor release (0.0.1 -> 0.1.0)
pnpm version:minor

# For major release (0.0.1 -> 1.0.0)
pnpm version:major
```

### 2. Build and Test

Ensure all packages build and tests pass:

```bash
# Build all packages
pnpm build

# Run all tests
pnpm test
```

### 3. Publish Packages

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

### 4. Verify Publication

Check that packages are available on npm:

```bash
npm view @reifydb/core
npm view @reifydb/client
```

## Package Dependencies

- `@reifydb/client` depends on `@reifydb/core` 
- When updating versions, ensure the client's dependency on core is updated if needed

## Manual Publishing (if needed)

For individual package publishing:

```bash
# For @reifydb/core
cd core
pnpm publish --access public

# For @reifydb/client  
cd client
pnpm publish --access public
```

## Troubleshooting

1. **Authentication issues**: Re-run `pnpm login`
2. **Scope access**: Ensure you're added to the @reifydb organization
3. **Version conflicts**: Check npm registry for existing versions
4. **Build failures**: Run `pnpm build` in the package directory
5. **Dependency issues**: Ensure workspace dependencies are resolved before publishing

## Notes

- The `prepublishOnly` script automatically runs build and tests before publishing
- All packages are configured with `"access": "public"` in publishConfig
- Repository information is included in each package.json for npm display