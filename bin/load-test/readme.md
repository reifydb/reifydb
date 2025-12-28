# reifydb-load-test

A redis-benchmark-style load testing tool for ReifyDB.

## Installation

Build from the workspace root:

```bash
cargo build -p load-test --release
```

The binary will be at `target/release/reifydb-load-test`.

## Usage

```bash
reifydb-load-test [OPTIONS] <PROTOCOL>
```

### Arguments

- `<PROTOCOL>` - Protocol to use: `http` or `ws`

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `-H, --host <HOST>` | Server host | `127.0.0.1` |
| `-p, --port <PORT>` | Server port | `8091` (http), `8090` (ws) |
| `-t, --token <TOKEN>` | Authentication token | - |
| `-c, --connections <N>` | Parallel connections | `50` |
| `-n, --requests <N>` | Total requests | `100000` |
| `-w, --workload <NAME>` | Workload preset | `mixed` |
| `--warmup <N>` | Warmup requests | `1000` |
| `--duration <DURATION>` | Run for duration (e.g., `30s`, `5m`) | - |
| `-q, --quiet` | Show only final summary | - |
| `--seed <N>` | RNG seed for reproducibility | random |
| `--table-size <N>` | Pre-populated table size | `10000` |

Environment variables: `REIFYDB_HOST`, `REIFYDB_PORT`, `REIFYDB_TOKEN`

## Workload Presets

| Preset | Description |
|--------|-------------|
| `ping` | Baseline latency test - executes `from [{ x: 1 }] map x` |
| `read` | Point lookups by primary key on `bench.users` |
| `write` | Insert operations to `bench.users` |
| `mixed` | 80% reads, 20% writes (default) |
| `scan` | Range scans with `filter id > N take 100` |
| `join` | Join queries across `bench.orders` and `bench.customers` |

## Examples

### Basic ping test (measure baseline latency)

```bash
reifydb-load-test http -w ping -n 10000
```

### Read workload with 100 connections

```bash
reifydb-load-test http -w read -c 100 -n 50000
```

### Write workload for 30 seconds

```bash
reifydb-load-test http -w write --duration 30s
```

### Mixed workload (default)

```bash
reifydb-load-test http
```

### WebSocket protocol with authentication

```bash
reifydb-load-test ws -t "your-auth-token" -n 100000
```

### Using environment variables

```bash
export REIFYDB_HOST=db.example.com
export REIFYDB_PORT=8080
export REIFYDB_TOKEN=secret

reifydb-load-test http -w mixed
```

### Reproducible run with seed

```bash
reifydb-load-test http -w read --seed 12345 -n 10000
```

### Custom table size for data-dependent workloads

```bash
reifydb-load-test http -w scan --table-size 100000
```

### Quiet mode (only final summary)

```bash
reifydb-load-test http -q -n 1000000
```

## Sample Output

```
====== MIXED (80% read, 20% write) ======
Host: 127.0.0.1:8080
Protocol: http
Connections: 50

100000 requests completed in 2.15 seconds

Throughput: 46,512 requests/second

Latency summary:
  min:       120 µs
  avg:       1.07 ms
  max:      45.23 ms
  p50:       890 µs
  p90:       1.56 ms
  p95:       2.34 ms
  p99:       5.67 ms
  p99.9:    12.45 ms

Successful: 100,000 / 100,000 (100.00% success rate)
Errors: 0 (0.00%)
```
