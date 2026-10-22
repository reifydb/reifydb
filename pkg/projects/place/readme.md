# ReifyDB Place

A shared 2048 x 2048 pixel canvas built on ReifyDB. Anyone can paint pixels,
commit up to 1,024 of them at once, and watch every other commit arrive live
through ReifyDB subscriptions. Runs as a single binary that embeds an in-memory
ReifyDB server.

## Quick start

```sh
cargo run -p reifydb-place-backend
cd web && pnpm install && pnpm dev
```

Open http://localhost:5173, pick a color, click to paint, then press Commit. The
browser connects straight to the ReifyDB WebSocket on `:8091`; set
`window.REIFYDB_WS_URL` to point it elsewhere. No account is needed: every
browser runs as the anonymous identity.

## Configuration

| Flag | Env | Default | Purpose |
|---|---|---|---|
| `--ws-bind` | `PLACE_WS_BIND` | `0.0.0.0:8091` | ReifyDB WebSocket subsystem |

Storage is in memory only. A restart reseeds a white canvas (about 12 s).

## Architecture

- One table, `place::live`, holds every pixel (4,194,304 rows), partitioned by
  64 x 64 tile (`tile_x = x / 64`, `tile_y = y / 64`). Every tile filter pins
  both partition keys, so reads and writes touch one partition.
- The binary seeds all 1,024 tiles at boot, one transaction per tile. A tile
  that is neither empty nor full stops the boot. After the last tile it marks
  the canvas ready in `place::state`; until then `place::commit` refuses every
  commit with `canvas not ready` (the WebSocket port is already open while the
  seed runs).
- A commit is one call, `CALL place::commit($pixels)`, where each pixel is packed
  into a `uint8` as `x * 2097152 + y * 256 + color`. The procedure checks the
  1,024-pixel cap and the canvas bounds, then updates each pixel. Any failed
  check rolls back the whole commit.
- Policies let the anonymous identity call `place::commit`, read and update
  `place::live`, and open subscriptions. Inserts and deletes are refused.
- The browser streams only the tiles on screen: per newly visible tile it opens
  a subscription with hydration off, then loads the tile with a query, then
  replays any change that arrived in between. Hidden tiles are unsubscribed.
  Hydration stays off because the engine's hydration scans the whole table.

## Tests

```sh
make test
```

Runs the backend tests, builds the `place_bridge` native addon, and runs the web
tests against the real engine and migrations in process.
