# ReifyDB WASM Browser Example

Interactive browser-based playground for ReifyDB WASM engine.

## Running the Example

### Option 1: Using Vite (Recommended)

```bash
# Install dependencies
npm install

# Run development server
npm run dev
```

Then open http://localhost:5173 in your browser.

### Option 2: Using Python HTTP Server

```bash
# Serve the directory
npm run serve
```

Then open http://localhost:8080 in your browser.

### Option 3: Using any HTTP server

You can use any HTTP server of your choice. Just make sure:
1. The server supports ES modules
2. WASM files are served with correct MIME type (`application/wasm`)

## Before Running

Make sure you've built the WASM module:

```bash
cd ../..
./build.sh
```

This will generate the WASM files in `pkg/wasm/web/`.

## Features

- 📝 Interactive query editor with syntax highlighting
- 📊 Results displayed in table format
- 📚 Pre-built example queries
- 📈 Query statistics (execution time, row count)
- ⌨️ Keyboard shortcuts (Ctrl/Cmd+Enter to run query)

## Example Queries

The playground includes examples for:
- Basic queries and filtering
- Creating tables and inserting data
- Aggregations (COUNT, AVG, MAX, MIN)
- GROUP BY operations
- JOIN queries

## Troubleshooting

### WASM module not loading

Make sure the path in `index.js` points to the correct WASM build:

```javascript
import init, { WasmEngine } from '../../pkg/wasm/web/reifydb_engine_wasm.js';
```

### CORS errors

WASM modules must be served over HTTP, not from `file://`. Use one of the serving options above.

### Module not found errors

Rebuild the WASM module:

```bash
cd ../..
./build.sh
```
