# ReifyDB Webassembly

WebAssembly bindings for [ReifyDB](https://github.com/reifydb/reifydb), enabling client-side database operations in browsers and Node.js.

## Features

- Browser Native: Run ReifyDB queries entirely in the browser
- In-Memory: Fast, pure-memory storage using HashMap and BTreeMap
- Lightweight: ~6 MB compressed bundle
- Full RQL: Complete ReifyDB Query Language support

## Quick Start

### Browser (ES Modules)

```javascript
import init, { WasmEngine } from './pkg/web/reifydb_engine_wasm.js';

await init();
const engine = new WasmEngine();

const results = await engine.query(`
  FROM [{ name: "Alice", age: 30 }]
  FILTER age > 25
`);
```

### Node.js

```javascript
const { WasmEngine } = require('./pkg/node/reifydb_engine_wasm.js');

const engine = new WasmEngine();
// ... use engine
```

## Building

### Prerequisites

```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install wasm-pack
curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh

# Add WASM target
rustup target add wasm32-unknown-unknown
```

### Build All Targets

```bash
./build.sh
```

This generates:
- `pkg/web/` - Browser with ES modules
- `pkg/node/` - Node.js
- `pkg/bundler/` - Webpack/Vite/etc.

## Examples

See the `examples/` directory:
- `examples/browser/` - Interactive playground

## Documentation

Full API documentation and guides: [docs/](./docs/)

## License

AGPL-3
