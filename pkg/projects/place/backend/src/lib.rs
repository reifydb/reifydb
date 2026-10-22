// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, env::var_os, path::PathBuf, sync::Arc};

use miniz_oxide::{deflate::compress_to_vec, inflate::decompress_to_vec_with_limit};
use reifydb::{
	Database, IdentityId, Value,
	arrow_array::{Array, ArrayRef, LargeBinaryArray},
	arrow_schema::FieldRef,
	core::{
		interface::catalog::config::ConfigKey,
		value::column::factory::{blob, int4, uint1},
	},
	routine::abi::{
		Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
		registry::RoutinesConfigurator,
	},
	value::{
		params::Params,
		value::{
			blob::Blob,
			column_view::{ColumnView, ViewData},
			duration::Duration,
			value_type::ValueType,
		},
	},
};

pub const TILE: usize = 64;
pub const TILES_PER_SIDE: u8 = 128;
pub const TILE_BYTES: usize = TILE * TILE;
pub const COLORS: i32 = 32;

const DEFLATE_LEVEL: u8 = 6;

pub fn encode_tile(pixels: &[u8]) -> Vec<u8> {
	compress_to_vec(pixels, DEFLATE_LEVEL)
}

pub fn decode_tile(blob: &[u8]) -> Result<Vec<u8>, String> {
	let pixels = decompress_to_vec_with_limit(blob, TILE_BYTES + 1)
		.map_err(|e| format!("tile does not decode: {e:?}"))?;
	if pixels.len() != TILE_BYTES {
		return Err(format!("tile decodes to {} bytes, expected {TILE_BYTES}", pixels.len()));
	}
	Ok(pixels)
}

pub fn migration_path() -> PathBuf {
	var_os("MIGRATION_PATH")
		.map(PathBuf::from)
		.unwrap_or_else(|| PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/migration")))
}

pub fn routines(routines: RoutinesConfigurator) -> RoutinesConfigurator {
	[
		("place::tile_x", TileFunction::TileX),
		("place::tile_y", TileFunction::TileY),
		("place::pixel_count", TileFunction::PixelCount),
		("place::paint_tile", TileFunction::PaintTile),
	]
	.into_iter()
	.fold(routines, |routines, (name, function)| {
		routines.register_function(Arc::new(TileRoutine {
			info: RoutineInfo::new(name),
			function,
		}))
	})
}

pub fn configs() -> [(ConfigKey, Value); 3] {
	[
		(ConfigKey::CdcTtlDuration, Value::Duration(Duration::from_seconds_const(60))),
		(ConfigKey::CdcTtlScanInterval, Value::Duration(Duration::from_seconds_const(10))),
		(ConfigKey::RetentionStartupGrace, Value::Duration(Duration::from_seconds_const(10))),
	]
}

pub struct TileEdit {
	pub tx: u8,
	pub ty: u8,
	pub pixels: Vec<(usize, u8)>,
}

pub fn encode_edit(tx: u8, ty: u8, pixels: &[(usize, u8)]) -> Vec<u8> {
	let mut edit = Vec::with_capacity(2 + pixels.len() * 3);
	edit.push(tx);
	edit.push(ty);
	for &(index, color) in pixels {
		edit.extend_from_slice(&(index as u16).to_le_bytes());
		edit.push(color);
	}
	edit
}

pub fn decode_edit(edit: &[u8]) -> Result<TileEdit, String> {
	if edit.len() < 2 || (edit.len() - 2) % 3 != 0 {
		return Err(format!("malformed tile edit of {} bytes", edit.len()));
	}
	let (tx, ty) = (edit[0], edit[1]);
	if tx >= TILES_PER_SIDE || ty >= TILES_PER_SIDE {
		return Err(format!("tile ({tx},{ty}) is outside the canvas"));
	}
	let mut pixels = Vec::with_capacity((edit.len() - 2) / 3);
	for pixel in edit[2..].chunks(3) {
		let index = u16::from_le_bytes([pixel[0], pixel[1]]) as usize;
		let color = pixel[2];
		if index >= TILE_BYTES {
			return Err(format!("pixel index {index} is outside the tile"));
		}
		if i32::from(color) >= COLORS {
			return Err(format!("color {color} is outside the palette"));
		}
		pixels.push((index, color));
	}
	Ok(TileEdit {
		tx,
		ty,
		pixels,
	})
}

#[derive(Clone, Copy)]
enum TileFunction {
	TileX,
	TileY,
	PixelCount,
	PaintTile,
}

struct TileRoutine {
	info: RoutineInfo,
	function: TileFunction,
}

impl<'a> Routine<FunctionContext<'a>> for TileRoutine {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		match self.function {
			TileFunction::TileX | TileFunction::TileY => ValueType::Uint1,
			TileFunction::PixelCount => ValueType::Int4,
			TileFunction::PaintTile => ValueType::Blob,
		}
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let failed = |reason: String| RoutineError::FunctionExecutionFailed {
			function: ctx.fragment.clone(),
			reason,
		};
		let last = args.len() - 1;
		let edits_view = ColumnView::try_from(&args[last])?;
		let edits = blobs(ctx, last, &edits_view)?;
		let decoded = (0..edits.len()).map(|row| decode_edit(edits.value(row)).map_err(failed));
		match self.function {
			TileFunction::TileX => {
				let keys =
					decoded.map(|edit| edit.map(|edit| edit.tx)).collect::<Result<Vec<_>, _>>()?;
				Ok(uint1(ctx.fragment.text(), keys))
			}
			TileFunction::TileY => {
				let keys =
					decoded.map(|edit| edit.map(|edit| edit.ty)).collect::<Result<Vec<_>, _>>()?;
				Ok(uint1(ctx.fragment.text(), keys))
			}
			TileFunction::PixelCount => {
				let counts = decoded
					.map(|edit| edit.map(|edit| edit.pixels.len() as i32))
					.collect::<Result<Vec<_>, _>>()?;
				Ok(int4(ctx.fragment.text(), counts))
			}
			TileFunction::PaintTile => {
				let edits = decoded.collect::<Result<Vec<_>, _>>()?;
				let tiles_view = ColumnView::try_from(&args[0])?;
				let tiles = blobs(ctx, 0, &tiles_view)?;
				let mut painted = Vec::with_capacity(edits.len());
				for (row, edit) in edits.into_iter().enumerate() {
					let mut pixels = decode_tile(tiles.value(row)).map_err(failed)?;
					for (index, color) in edit.pixels {
						pixels[index] = color;
					}
					painted.push(Blob::new(encode_tile(&pixels)));
				}
				Ok(blob(ctx.fragment.text(), painted))
			}
		}
	}
}

impl Function for TileRoutine {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		match self.function {
			TileFunction::PaintTile => Arity::Exact(2),
			TileFunction::TileX | TileFunction::TileY | TileFunction::PixelCount => Arity::Exact(1),
		}
	}
}

fn blobs<'v>(
	ctx: &FunctionContext<'_>,
	index: usize,
	view: &'v ColumnView<'v>,
) -> Result<&'v LargeBinaryArray, RoutineError> {
	match &view.data {
		ViewData::Blob {
			container,
			..
		} => Ok(container),
		_ => Err(RoutineError::FunctionInvalidArgumentType {
			function: ctx.fragment.clone(),
			argument_index: index,
			expected: vec![ValueType::Blob],
			actual: view.get_type(),
		}),
	}
}

fn named(entries: &[(&str, Value)]) -> Params {
	let map: HashMap<String, Value> = entries.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect();
	Params::from(map)
}

pub fn seed(db: &Database) -> Result<(), String> {
	for ty in 0..TILES_PER_SIDE {
		let mut missing = Vec::new();
		for tx in 0..TILES_PER_SIDE {
			if !tile_stored(db, tx, ty)? {
				missing.push(tx);
			}
		}
		insert_tiles(db, ty, &missing)?;
	}
	mark_ready(db)
}

fn mark_ready(db: &Database) -> Result<(), String> {
	let frames = db
		.query_as_root(
			"from place::state filter { key == \"canvas\" } aggregate { n: math::count(key) }",
			Params::None,
		)
		.map_err(|e| format!("read canvas state: {e:?}"))?;
	let marked = match frames.first().and_then(|f| f.rows().next()) {
		None => 0,
		Some(row) => row.get::<i64>("n").map_err(|e| format!("read canvas state count: {e:?}"))?.unwrap_or(0),
	};
	if marked > 0 {
		return Ok(());
	}
	db.command_as_root("insert place::state [{ key: \"canvas\", ready: true }]", Params::None)
		.map(|_| ())
		.map_err(|e| format!("mark canvas ready: {e:?}"))
}

fn tile_stored(db: &Database, tx: u8, ty: u8) -> Result<bool, String> {
	let frames = db
		.query_as_root(
			"from place::tile filter { tile_x == $tx and tile_y == $ty } map { tile_x, pixels }",
			named(&[("tx", Value::Uint1(tx)), ("ty", Value::Uint1(ty))]),
		)
		.map_err(|e| format!("read tile ({tx},{ty}): {e:?}"))?;
	let mut rows = frames.iter().flat_map(|frame| frame.rows());
	let Some(row) = rows.next() else {
		return Ok(false);
	};
	if rows.next().is_some() {
		return Err(format!("tile ({tx},{ty}) is stored more than once"));
	}
	let pixels = row
		.get::<Blob>("pixels")
		.map_err(|e| format!("read pixels of tile ({tx},{ty}): {e:?}"))?
		.ok_or_else(|| format!("tile ({tx},{ty}) has no pixels"))?;
	decode_tile(pixels.as_bytes()).map(|_| true).map_err(|e| format!("tile ({tx},{ty}): {e}"))
}

fn insert_tiles(db: &Database, ty: u8, columns: &[u8]) -> Result<(), String> {
	if columns.is_empty() {
		return Ok(());
	}
	let white = Value::Blob(Blob::new(encode_tile(&[0u8; TILE_BYTES])));
	let rows = columns.iter().map(|&tx| {
		named(&[("tile_x", Value::Uint1(tx)), ("tile_y", Value::Uint1(ty)), ("pixels", white.clone())])
	});
	let mut b = db.engine().bulk_insert(IdentityId::root());
	b.table("place::tile").rows(rows).done();
	b.execute().map(|_| ()).map_err(|e| format!("seed tile row {ty}: {e:?}"))
}
