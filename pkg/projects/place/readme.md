# ReifyDB Place

A shared 8192 x 8192 pixel canvas built on ReifyDB. Anyone can paint pixels,
commit up to 1,024 of them at once, and watch every other commit arrive live
through ReifyDB subscriptions. Runs as a single binary that embeds an in-memory
ReifyDB server.

## Quick start

```sh
make backend
make web
```

Open http://localhost:5173, pick a color, click to paint, then press Commit. The
browser connects straight to the ReifyDB WebSocket on `:8091`; set
`window.REIFYDB_WS_URL` to point it elsewhere. No account is needed: every
browser runs as the anonymous identity.

## Configuration

| Flag | Env | Default | Purpose |
|---|---|---|---|
| `--ws-bind` | `PLACE_WS_BIND` | `0.0.0.0:8091` | ReifyDB WebSocket subsystem |

Storage is in memory only. A restart reseeds a white canvas (well under a second).
The binary trims the change log after 60 s (`CdcTtlDuration`), scanning every 10 s
and starting 10 s after boot; clients reload a tile when they resubscribe, so they
never need older changes.

## Architecture

- One table, `place::tile`, holds the canvas as 16,384 rows: one row per 64 x 64
  tile, partitioned by `(tile_x, tile_y)`. The tile's 4,096 pixels (one byte per
  pixel, row-major, a palette index 0-31) are stored deflate-compressed, so a blank
  tile takes about 20 bytes. Every tile filter pins both partition keys, so reads
  and writes touch one partition.
- The binary seeds all tiles at boot. A tile stored twice or with the wrong size
  stops the boot. After the last tile it marks the canvas ready in `place::state`;
  until then `place::commit` refuses every commit with `canvas not ready` (the
  WebSocket port is already open while the seed runs).
- A commit is one call, `CALL place::commit($tiles)`, with one blob per touched
  tile: `[tile_x, tile_y, (index u16 little-endian, color u8)...]`, where index is
  `(y % 64) * 64 + x % 64`. The procedure sums the pixels with
  `place::pixel_count` to enforce the 1,024-pixel cap, then runs one update per
  tile with `place::paint_tile(pixels, edit)` (decompress, apply every pixel of
  the edit, recompress). The native functions refuse a tile outside the canvas,
  an index outside the tile, a color outside the palette or a malformed edit, and
  any refusal rolls back the whole commit. They are registered by the binary and
  the test bridge.
- The palette is the 32-color palette of r/place 2022, with white at index 0 so a
  freshly seeded canvas is white.
- Two commits that touch the same tile at the same time conflict; the browser
  retries a conflicted commit up to 5 times with exponential backoff and jitter,
  and keeps the pixels pending if it still fails.
- Policies let the anonymous identity call `place::commit`, read and update
  `place::tile`, and open subscriptions. Inserts and deletes are refused.
- The browser streams only the tiles on screen: per newly visible tile it opens a
  subscription with hydration off and loads the tile with a query, then
  decompresses it with the browser's `DecompressionStream`. Every snapshot
  and change carries the tile's `#commit_version`, and a tile is drawn only when
  its version is newer than the one held, so a late snapshot never rolls a tile
  back. Hidden tiles are unsubscribed and dropped from memory.
- The browser draws only visible tiles onto a viewport-sized canvas, from a cached
  64 x 64 bitmap per loaded tile, so memory follows the screen, not the canvas.

## Tests

```sh
make test
```

Runs the backend tests, builds the `place_bridge` native addon, and runs the web
tests against the real engine and migrations in process.
