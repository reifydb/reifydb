// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, io::Read};

use flate2::read::ZlibDecoder;
use reifydb::{
	Clock, Database, IdentityId, MockClock, RuntimeConfig, Value, WithSubsystem, server,
	value::{params::Params, value::blob::Blob},
};
use reifydb_canvas::{TILE, TILE_BYTES, encode_tile, migration_path, routines, tile_key};

fn build() -> Database {
	let config = RuntimeConfig {
		clock: Clock::Mock(MockClock::from_millis(1_700_000_000_000)),
		..Default::default()
	};
	server::memory()
		.with_runtime_config(config)
		.with_flow(|f| f)
		.with_routines(routines)
		.with_migrations(migration_path())
		.build()
		.expect("build memory db")
}

fn put_tile(db: &Database, tx: u8, ty: u8, pixels: &[u8]) {
	let mut map = HashMap::new();
	map.insert("pixels".to_string(), Value::Blob(Blob::new(encode_tile(pixels))));
	let rql = format!("insert canvas::tile [{{ tile: {}, pixels: $pixels }}]", tile_key(tx, ty));
	let r = db.engine().command_as(IdentityId::root(), &rql, Params::from(map));
	if let Some(e) = r.error {
		panic!("insert tile ({tx},{ty}) failed: {e:?}");
	}
}

fn png_of(db: &Database, tiles: &[(u8, u8)], scale: &str) -> Result<Vec<u8>, String> {
	let keys = Value::List(tiles.iter().map(|&(tx, ty)| Value::Uint2(tile_key(tx, ty))).collect());
	let mut map = HashMap::new();
	map.insert("keys".to_string(), keys);
	let rql = format!(
		"from canvas::tile filter {{ tile in $keys }} aggregate {{ png: canvas::png(canvas::pack(tile, pixels), {scale}) }}"
	);
	let r = db.engine().query_as(IdentityId::anonymous(), &rql, Params::from(map));
	if let Some(e) = r.error {
		return Err(format!("{e:?}"));
	}
	let row = r.frames.first().and_then(|f| f.rows().next()).ok_or("query returned no row")?;
	let png = row.get::<Blob>("png").map_err(|e| format!("{e:?}"))?.ok_or("png is none")?;
	Ok(png.as_bytes().to_vec())
}

struct Decoded {
	width: usize,
	height: usize,
	palette: Vec<u8>,
	indexes: Vec<u8>,
}

fn decode(png: &[u8]) -> Decoded {
	// Parsed with outside CRC and zlib code, so an encoder bug in either cannot hide behind a matching decoder bug.
	assert_eq!(&png[..8], &[0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a], "PNG signature must be exact");
	let mut at = 8;
	let mut chunks = Vec::new();
	while at < png.len() {
		let len = u32::from_be_bytes(png[at..at + 4].try_into().unwrap()) as usize;
		let kind = &png[at + 4..at + 8];
		let data = &png[at + 8..at + 8 + len];
		let crc = u32::from_be_bytes(png[at + 8 + len..at + 12 + len].try_into().unwrap());
		assert_eq!(crc, crc32fast::hash(&png[at + 4..at + 8 + len]), "chunk {:?} CRC must match", kind);
		chunks.push((kind.to_vec(), data.to_vec()));
		at += 12 + len;
	}
	let kinds: Vec<&[u8]> = chunks.iter().map(|(k, _)| k.as_slice()).collect();
	assert_eq!(
		kinds,
		vec![&b"IHDR"[..], b"PLTE", b"IDAT", b"IEND"],
		"chunks must be exactly IHDR, PLTE, IDAT, IEND"
	);
	let ihdr = &chunks[0].1;
	assert_eq!(ihdr.len(), 13, "IHDR must be 13 bytes");
	let width = u32::from_be_bytes(ihdr[0..4].try_into().unwrap()) as usize;
	let height = u32::from_be_bytes(ihdr[4..8].try_into().unwrap()) as usize;
	assert_eq!(&ihdr[8..], &[8, 3, 0, 0, 0], "must be 8-bit palette, deflate, no filter method, no interlace");
	assert!(chunks[3].1.is_empty(), "IEND must be empty");
	let mut raw = Vec::new();
	ZlibDecoder::new(chunks[2].1.as_slice()).read_to_end(&mut raw).expect("IDAT must be a valid zlib stream");
	assert_eq!(raw.len(), (width + 1) * height, "IDAT must hold one filter byte plus one row per line");
	let mut indexes = Vec::with_capacity(width * height);
	for line in raw.chunks(width + 1) {
		assert_eq!(line[0], 0, "every row must use filter type none");
		indexes.extend_from_slice(&line[1..]);
	}
	Decoded {
		width,
		height,
		palette: chunks[1].1.clone(),
		indexes,
	}
}

fn pattern(tx: u8, ty: u8) -> Vec<u8> {
	(0..TILE_BYTES)
		.map(|i| (((i % TILE) * 7 + (i / TILE) * 3 + tx as usize * 5 + ty as usize) % 32) as u8)
		.collect()
}

#[test]
fn scale_one_png_shows_every_stored_index_in_place() {
	// Two side-by-side tiles with distinct patterns: a swapped, shifted or transposed tile changes some index.
	let db = build();
	put_tile(&db, 3, 5, &pattern(3, 5));
	put_tile(&db, 4, 5, &pattern(4, 5));
	let png = decode(&png_of(&db, &[(3, 5), (4, 5)], "1").expect("png query must succeed"));
	assert_eq!((png.width, png.height), (2 * TILE, TILE), "two tiles across must give 128x64");
	for y in 0..TILE {
		for x in 0..2 * TILE {
			let (tx, lx) = if x < TILE {
				(3, x)
			} else {
				(4, x - TILE)
			};
			assert_eq!(
				png.indexes[y * png.width + x],
				pattern(tx, 5)[y * TILE + lx],
				"pixel ({x},{y}) must be the stored index"
			);
		}
	}
}

#[test]
fn scaled_png_takes_the_mode_and_the_lowest_index_on_a_tie() {
	// Block 0 has a 9-of-16 majority; block 1 is an 8/8 tie that must resolve to the lower index.
	let db = build();
	let mut tile = vec![0u8; TILE_BYTES];
	for i in 0..16 {
		tile[(i / 4) * TILE + i % 4] = if i < 9 {
			5
		} else {
			7
		};
		tile[(i / 4) * TILE + 4 + i % 4] = if i < 8 {
			9
		} else {
			2
		};
	}
	put_tile(&db, 0, 0, &tile);
	let png = decode(&png_of(&db, &[(0, 0)], "4").expect("png query must succeed"));
	assert_eq!((png.width, png.height), (16, 16), "scale 4 must turn one tile into 16x16");
	assert_eq!(png.indexes[0], 5, "a block must become its most common index");
	assert_eq!(png.indexes[1], 2, "a tie must resolve to the lower index");
	assert_eq!(png.indexes[2], 0, "an untouched block must stay white");
}

#[test]
fn palette_chunk_carries_the_published_canvas_colors() {
	// Expected hex comes from the published skill table, not the crate constant, so a typo in either side fails.
	let db = build();
	put_tile(&db, 0, 0, &[0u8; TILE_BYTES]);
	let png = decode(&png_of(&db, &[(0, 0)], "64").expect("png query must succeed"));
	assert_eq!(png.palette.len(), 32 * 3, "palette must have exactly 32 colors");
	let rgb = |i: usize| &png.palette[i * 3..i * 3 + 3];
	assert_eq!(rgb(0), &[0xff, 0xff, 0xff], "index 0 must be white");
	assert_eq!(rgb(3), &[0xff, 0x45, 0x00], "index 3 must be #ff4500");
	assert_eq!(rgb(13), &[0x24, 0x50, 0xa4], "index 13 must be #2450a4");
	assert_eq!(rgb(28), &[0x00, 0x00, 0x00], "index 28 must be black");
	assert_eq!(rgb(31), &[0xd4, 0xd7, 0xd9], "index 31 must be #d4d7d9");
	assert_eq!((png.width, png.height), (1, 1), "scale 64 must shrink a tile to one pixel");
}

#[test]
fn a_gap_in_the_requested_rectangle_is_an_error_not_a_white_hole() {
	// Tiles are always seeded, so a hole means the caller's key list is wrong and must fail loud.
	let db = build();
	put_tile(&db, 0, 0, &[1u8; TILE_BYTES]);
	put_tile(&db, 2, 0, &[1u8; TILE_BYTES]);
	let err = png_of(&db, &[(0, 0), (2, 0)], "1").expect_err("a missing tile inside the box must fail");
	assert!(err.contains("tile (1,0) is missing"), "error must name the missing tile, got: {err}");
}

#[test]
fn a_scale_that_does_not_divide_the_tile_is_rejected() {
	// Scale 3 would leave a partial block at the tile edge; it must be refused, not rounded.
	let db = build();
	put_tile(&db, 0, 0, &[0u8; TILE_BYTES]);
	let err = png_of(&db, &[(0, 0)], "3").expect_err("scale 3 must fail");
	assert!(err.contains("scale 3 must be one of"), "error must explain the allowed scales, got: {err}");
}
