// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	env::var_os,
	mem::take,
	path::PathBuf,
	sync::Arc,
};

use miniz_oxide::{
	deflate::{compress_to_vec, compress_to_vec_zlib},
	inflate::decompress_to_vec_with_limit,
};
use reifydb::{
	Database, IdentityId, Value,
	arrow_array::{Array, ArrayRef, LargeBinaryArray},
	arrow_schema::FieldRef,
	core::{
		interface::catalog::config::ConfigKey,
		value::column::{
			factory::{any_typed, blob, int4, uint2},
			view::group_by::{GroupId, GroupRows, GroupSlots},
		},
	},
	routine::abi::{
		Accumulator, Arity, Function, FunctionKind, LiteralArgument, LiteralKind, Prepared, Routine,
		RoutineInfo,
		broadcast::row_of,
		context::{FunctionContext, check_lengths},
		error::RoutineError,
		registry::RoutinesConfigurator,
	},
	value::{
		fragment::Fragment,
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
pub const COLORS: i32 = 64;
pub const CANVAS: u32 = (TILE * TILES_PER_SIDE as usize) as u32;
pub const MAX_SPRITE: usize = 128;

const DEFLATE_LEVEL: u8 = 3;

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
		("canvas::tile_key", TileFunction::TileKey),
		("canvas::edit_pixels", TileFunction::EditPixels),
		("canvas::tile_keys", TileFunction::TileKeys),
		("canvas::pixel_count", TileFunction::PixelCount),
		("canvas::paint", TileFunction::Paint),
		("canvas::parse_pixels", TileFunction::ParsePixels),
		("canvas::parse_sprite", TileFunction::ParseSprite),
		("canvas::pack", TileFunction::Pack),
	]
	.into_iter()
	.fold(routines, |routines, (name, function)| {
		routines.register_function(Arc::new(TileRoutine {
			info: RoutineInfo::new(name),
			function,
		}))
	})
	.register_function(Arc::new(PngRoutine {
		info: RoutineInfo::new("canvas::png"),
	}))
}

pub const PALETTE: [u32; COLORS as usize] = [
	0xffffff, 0x6d001a, 0xbe0039, 0xff4500, 0xffa800, 0xffd635, 0xfff8b8, 0x00a368, 0x00cc78, 0x7eed56, 0x00756f,
	0x009eaa, 0x00ccc0, 0x2450a4, 0x3690ea, 0x51e9f4, 0x493ac1, 0x6a5cff, 0x94b3ff, 0x811e9f, 0xb44ac0, 0xe4abff,
	0xde107f, 0xff3881, 0xff99aa, 0x6d482f, 0x9c6926, 0xffb470, 0x000000, 0x515252, 0x898d90, 0xd4d7d9, 0xffdba1,
	0xeec5a6, 0xd99a6c, 0xb3774e, 0x8c4f2e, 0x5a3220, 0x152c5e, 0x004d2c, 0x4a0f1d, 0x003c41, 0x30175a, 0x3e230a,
	0x454a16, 0x4a1441, 0x7a7f2a, 0x8fae8b, 0x688cb1, 0xc25b36, 0xc9a227, 0x4f6276, 0x9c6b8a, 0xc8b98a, 0xbff0cf,
	0xb1e7ff, 0xdfd0ff, 0xffd0da, 0xffe9d6, 0xc8fefd, 0x141516, 0xb0b3b5, 0x6d7072, 0x272b2f,
];

pub fn pack_tile(key: u16, blob: &[u8]) -> Vec<u8> {
	let mut packed = Vec::with_capacity(2 + blob.len());
	packed.extend_from_slice(&key.to_le_bytes());
	packed.extend_from_slice(blob);
	packed
}

pub fn shrink(tile: Vec<u8>, scale: usize) -> Vec<u8> {
	if scale == 1 {
		return tile;
	}
	let side = TILE / scale;
	let mut out = Vec::with_capacity(side * side);
	let mut counts = [0u32; COLORS as usize];
	let mut used = [0u8; COLORS as usize];
	for by in 0..side {
		for bx in 0..side {
			let mut n = 0;
			for y in by * scale..(by + 1) * scale {
				for &color in &tile[y * TILE + bx * scale..y * TILE + (bx + 1) * scale] {
					if counts[color as usize] == 0 {
						used[n] = color;
						n += 1;
					}
					counts[color as usize] += 1;
				}
			}
			let mut best = used[0];
			for &color in &used[1..n] {
				let (c, b) = (counts[color as usize], counts[best as usize]);
				if c > b || (c == b && color < best) {
					best = color;
				}
			}
			for &color in &used[..n] {
				counts[color as usize] = 0;
			}
			out.push(best);
		}
	}
	out
}

pub fn encode_png(width: u32, height: u32, indexes: &[u8]) -> Vec<u8> {
	let mut png = vec![0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a];
	let mut header = Vec::with_capacity(13);
	header.extend_from_slice(&width.to_be_bytes());
	header.extend_from_slice(&height.to_be_bytes());
	header.extend_from_slice(&[8, 3, 0, 0, 0]);
	png_chunk(&mut png, b"IHDR", &header);
	let palette: Vec<u8> =
		PALETTE.iter().flat_map(|rgb| [(rgb >> 16) as u8, (rgb >> 8) as u8, *rgb as u8]).collect();
	png_chunk(&mut png, b"PLTE", &palette);
	let row = width as usize;
	let mut raw = Vec::with_capacity((row + 1) * height as usize);
	for line in indexes.chunks(row) {
		raw.push(0);
		raw.extend_from_slice(line);
	}
	png_chunk(&mut png, b"IDAT", &compress_to_vec_zlib(&raw, DEFLATE_LEVEL));
	png_chunk(&mut png, b"IEND", &[]);
	png
}

fn png_chunk(png: &mut Vec<u8>, kind: &[u8; 4], data: &[u8]) {
	png.extend_from_slice(&(data.len() as u32).to_be_bytes());
	let start = png.len();
	png.extend_from_slice(kind);
	png.extend_from_slice(data);
	let crc = crc32(&png[start..]);
	png.extend_from_slice(&crc.to_be_bytes());
}

const CRC_TABLE: [u32; 256] = {
	let mut table = [0u32; 256];
	let mut n = 0;
	while n < 256 {
		let mut c = n as u32;
		let mut k = 0;
		while k < 8 {
			c = if c & 1 == 1 {
				0xedb8_8320 ^ (c >> 1)
			} else {
				c >> 1
			};
			k += 1;
		}
		table[n] = c;
		n += 1;
	}
	table
};

fn crc32(bytes: &[u8]) -> u32 {
	let mut c = 0xffff_ffffu32;
	for &b in bytes {
		c = CRC_TABLE[((c ^ u32::from(b)) & 0xff) as usize] ^ (c >> 8);
	}
	c ^ 0xffff_ffff
}

pub fn render_png(tiles: &[(u16, Vec<u8>)], scale: usize) -> Result<Vec<u8>, String> {
	let side = u16::from(TILES_PER_SIDE);
	let (mut x0, mut y0, mut x1, mut y1) = (side, side, 0u16, 0u16);
	for &(key, _) in tiles {
		let (tx, ty) = (key % side, key / side);
		x0 = x0.min(tx);
		y0 = y0.min(ty);
		x1 = x1.max(tx);
		y1 = y1.max(ty);
	}
	if tiles.is_empty() {
		return Err("no tiles to render".to_string());
	}
	let (w, h) = (usize::from(x1 - x0 + 1), usize::from(y1 - y0 + 1));
	let mut slots: Vec<Option<&[u8]>> = vec![None; w * h];
	for (key, pixels) in tiles {
		let (tx, ty) = (key % side, key / side);
		let slot = &mut slots[usize::from(ty - y0) * w + usize::from(tx - x0)];
		if slot.is_some() {
			return Err(format!("tile ({tx},{ty}) is given more than once"));
		}
		*slot = Some(pixels);
	}
	let block = TILE / scale;
	let (width, height) = (w * block, h * block);
	let mut image = vec![0u8; width * height];
	for (i, slot) in slots.iter().enumerate() {
		let (col, row) = (i % w, i / w);
		let pixels = slot.ok_or_else(|| {
			format!(
				"tile ({},{}) is missing from the area ({x0},{y0})..({x1},{y1})",
				x0 + col as u16,
				y0 + row as u16
			)
		})?;
		for y in 0..block {
			let at = (row * block + y) * width + col * block;
			image[at..at + block].copy_from_slice(&pixels[y * block..(y + 1) * block]);
		}
	}
	Ok(encode_png(width as u32, height as u32, &image))
}

struct PngRoutine {
	info: RoutineInfo,
}

impl<'a> Routine<FunctionContext<'a>> for PngRoutine {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Blob
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		_args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		Err(failed(ctx, "canvas::png is an aggregate, use it inside aggregate { }".to_string()))
	}
}

impl Function for PngRoutine {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Aggregate]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(2)
	}

	fn max_literal_arguments(&self) -> usize {
		1
	}

	fn accumulator(
		&self,
		ctx: &mut FunctionContext<'_>,
		literals: &[LiteralArgument],
	) -> Result<Option<Box<dyn Accumulator>>, RoutineError> {
		let [literal] = literals else {
			return Err(failed(
				ctx,
				"canvas::png takes a scale: canvas::png(canvas::pack(tile, pixels), 4)".to_string(),
			));
		};
		let scale = match literal.kind {
			LiteralKind::Number => literal.fragment.text().parse::<usize>().ok(),
			_ => None,
		}
		.filter(|scale| *scale >= 1 && TILE % scale == 0)
		.ok_or_else(|| {
			failed(ctx, format!("scale {} must be one of 1, 2, 4, 8, 16, 32, 64", literal.fragment.text()))
		})?;
		Ok(Some(Box::new(PngAccumulator {
			fragment: ctx.fragment.clone(),
			scale,
			tiles: GroupSlots::new(),
		})))
	}
}

struct PngAccumulator {
	fragment: Fragment,
	scale: usize,
	tiles: GroupSlots<Vec<(u16, Vec<u8>)>>,
}

impl PngAccumulator {
	fn failed(&self, reason: String) -> RoutineError {
		RoutineError::FunctionExecutionFailed {
			function: self.fragment.clone(),
			reason,
		}
	}
}

impl Accumulator for PngAccumulator {
	fn update(&mut self, args: &[(FieldRef, ArrayRef)], groups: &GroupRows) -> Result<(), RoutineError> {
		let view = ColumnView::try_from(&args[0])?;
		let ViewData::Blob {
			container,
			..
		} = &view.data
		else {
			return Err(self.failed(format!(
				"canvas::png expects canvas::pack(tile, pixels), got {:?}",
				view.get_type()
			)));
		};
		for &(group, ref indices) in groups.iter() {
			let mut decoded = Vec::with_capacity(indices.len());
			for &i in indices {
				let packed = container.value(row_of(container.len(), i));
				if packed.len() < 2 {
					return Err(self
						.failed(format!("packed tile of {} bytes has no key", packed.len())));
				}
				let key = u16::from_le_bytes([packed[0], packed[1]]);
				let pixels = shrink(
					decode_tile(&packed[2..]).map_err(|reason| self.failed(reason))?,
					self.scale,
				);
				decoded.push((key, pixels));
			}
			self.tiles.or_insert(group, Vec::new()).extend(decoded);
		}
		Ok(())
	}

	fn finalize(&mut self) -> Result<(Vec<GroupId>, (FieldRef, ArrayRef)), RoutineError> {
		let mut keys = Vec::with_capacity(self.tiles.len());
		let mut pngs = Vec::with_capacity(self.tiles.len());
		for (group, tiles) in take(&mut self.tiles) {
			keys.push(group);
			pngs.push(Blob::new(render_png(&tiles, self.scale).map_err(|reason| self.failed(reason))?));
		}
		Ok((keys, blob(self.kind_name(), pngs)))
	}

	fn heap_size(&self) -> usize {
		self.tiles.len() * TILE_BYTES
	}

	fn kind_name(&self) -> &'static str {
		"canvas::png"
	}
}

pub fn configs() -> [(ConfigKey, Value); 1] {
	[(ConfigKey::RetentionStartupGrace, Value::Duration(Duration::from_seconds_const(10)))]
}

pub struct TileEdit {
	pub tx: u8,
	pub ty: u8,
	pub pixels: Vec<(usize, u8)>,
}

pub fn tile_key(tx: u8, ty: u8) -> u16 {
	u16::from(ty) * u16::from(TILES_PER_SIDE) + u16::from(tx)
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
	TileKey,
	EditPixels,
	TileKeys,
	PixelCount,
	Paint,
	ParsePixels,
	ParseSprite,
	Pack,
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
			TileFunction::TileKey | TileFunction::EditPixels => ValueType::Uint2,
			TileFunction::TileKeys => ValueType::List(Box::new(ValueType::Uint2)),
			TileFunction::PixelCount => ValueType::Int4,
			TileFunction::Paint | TileFunction::Pack => ValueType::Blob,
			TileFunction::ParsePixels | TileFunction::ParseSprite => edit_list_type(),
		}
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let rows = ctx.row_count;
		let tiles = &args[args.len() - 1];
		match self.function {
			TileFunction::TileKey => edit_rows(ctx, tiles, |edit| tile_key(edit.tx, edit.ty)),
			TileFunction::EditPixels => edit_rows(ctx, tiles, |edit| edit.pixels.len() as u16),
			TileFunction::TileKeys => {
				let edits = decode_edits(ctx, tiles)?;
				let mut keys: Vec<u16> = edits.iter().map(|edit| tile_key(edit.tx, edit.ty)).collect();
				keys.sort_unstable();
				keys.dedup();
				let list = Value::List(keys.into_iter().map(Value::Uint2).collect());
				Ok(any_typed(
					ctx.fragment.text(),
					(0..rows).map(|_| list.clone()),
					ValueType::List(Box::new(ValueType::Uint2)),
				))
			}
			TileFunction::PixelCount => {
				let edits = decode_edits(ctx, tiles)?;
				let count = edits.iter().map(|edit| edit.pixels.len() as i32).sum::<i32>();
				Ok(int4(ctx.fragment.text(), (0..rows).map(|_| count)))
			}
			TileFunction::Paint => paint_rows(ctx, &group_by_tile(decode_edits(ctx, tiles)?), args),
			TileFunction::Pack => pack_rows(ctx, args),
			TileFunction::ParsePixels => {
				let pixels = texts(&first_value(&args[0])?).map_err(|reason| failed(ctx, reason))?;
				edit_list_rows(ctx, parse_pixels(&pixels).map_err(|reason| failed(ctx, reason))?)
			}
			TileFunction::ParseSprite => {
				let x = coordinate(&first_value(&args[0])?).map_err(|reason| failed(ctx, reason))?;
				let y = coordinate(&first_value(&args[1])?).map_err(|reason| failed(ctx, reason))?;
				let rows = texts(&first_value(&args[2])?).map_err(|reason| failed(ctx, reason))?;
				let legend = texts(&first_value(&args[3])?).map_err(|reason| failed(ctx, reason))?;
				edit_list_rows(
					ctx,
					parse_sprite(x, y, &rows, &legend).map_err(|reason| failed(ctx, reason))?,
				)
			}
		}
	}
}

type EditsByTile = HashMap<u16, Vec<(usize, u8)>>;

fn failed(ctx: &FunctionContext<'_>, reason: String) -> RoutineError {
	RoutineError::FunctionExecutionFailed {
		function: ctx.fragment.clone(),
		reason,
	}
}

fn decode_edits(ctx: &FunctionContext<'_>, tiles: &(FieldRef, ArrayRef)) -> Result<Vec<TileEdit>, RoutineError> {
	edit_list(&ColumnView::try_from(tiles)?.get_value(0))
		.map_err(|reason| failed(ctx, reason))?
		.into_iter()
		.map(|edit| decode_edit(&edit))
		.collect::<Result<Vec<_>, _>>()
		.map_err(|reason| failed(ctx, reason))
}

fn edit_rows(
	ctx: &FunctionContext<'_>,
	tiles: &(FieldRef, ArrayRef),
	value: fn(&TileEdit) -> u16,
) -> Result<(FieldRef, ArrayRef), RoutineError> {
	let view = ColumnView::try_from(tiles)?;
	let edits = blobs(ctx, 0, &view)?;
	let mut keys = Vec::with_capacity(ctx.row_count);
	for row in 0..ctx.row_count {
		let edit = decode_edit(edits.value(row_of(edits.len(), row))).map_err(|reason| failed(ctx, reason))?;
		keys.push(value(&edit));
	}
	Ok(uint2(ctx.fragment.text(), keys))
}

fn group_by_tile(edits: Vec<TileEdit>) -> EditsByTile {
	let mut by_tile: EditsByTile = HashMap::new();
	for edit in edits {
		by_tile.entry(tile_key(edit.tx, edit.ty)).or_default().extend(edit.pixels);
	}
	by_tile
}

fn paint_rows(
	ctx: &FunctionContext<'_>,
	by_tile: &EditsByTile,
	args: &[(FieldRef, ArrayRef)],
) -> Result<(FieldRef, ArrayRef), RoutineError> {
	let tiles_view = ColumnView::try_from(&args[0])?;
	let tiles = blobs(ctx, 0, &tiles_view)?;
	let keys_view = ColumnView::try_from(&args[1])?;
	let keys = match &keys_view.data {
		ViewData::Uint2(keys) => keys,
		_ => {
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![ValueType::Uint2],
				actual: keys_view.get_type(),
			});
		}
	};
	let mut painted = Vec::with_capacity(ctx.row_count);
	for row in 0..ctx.row_count {
		let key = keys.value(row_of(keys.len(), row));
		let edit = by_tile.get(&key).ok_or_else(|| failed(ctx, format!("tile {key} has no edit")))?;
		let mut pixels =
			decode_tile(tiles.value(row_of(tiles.len(), row))).map_err(|reason| failed(ctx, reason))?;
		for &(index, color) in edit {
			pixels[index] = color;
		}
		painted.push(Blob::new(encode_tile(&pixels)));
	}
	Ok(blob(ctx.fragment.text(), painted))
}

fn pack_rows(ctx: &FunctionContext<'_>, args: &[(FieldRef, ArrayRef)]) -> Result<(FieldRef, ArrayRef), RoutineError> {
	let keys_view = ColumnView::try_from(&args[0])?;
	let ViewData::Uint2(keys) = &keys_view.data else {
		return Err(RoutineError::FunctionInvalidArgumentType {
			function: ctx.fragment.clone(),
			argument_index: 0,
			expected: vec![ValueType::Uint2],
			actual: keys_view.get_type(),
		});
	};
	let tiles_view = ColumnView::try_from(&args[1])?;
	let tiles = blobs(ctx, 1, &tiles_view)?;
	let packed = (0..ctx.row_count).map(|row| {
		Blob::new(pack_tile(keys.value(row_of(keys.len(), row)), tiles.value(row_of(tiles.len(), row))))
	});
	Ok(blob(ctx.fragment.text(), packed.collect::<Vec<_>>()))
}

impl Function for TileRoutine {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		match self.function {
			TileFunction::Paint => Arity::Exact(3),
			TileFunction::Pack => Arity::Exact(2),
			TileFunction::ParseSprite => Arity::Exact(4),
			TileFunction::ParsePixels => Arity::Exact(1),
			TileFunction::TileKey
			| TileFunction::EditPixels
			| TileFunction::TileKeys
			| TileFunction::PixelCount => Arity::Exact(1),
		}
	}

	fn prepare(
		&self,
		ctx: &mut FunctionContext<'_>,
		constants: &[Option<&(FieldRef, ArrayRef)>],
	) -> Result<Option<Prepared>, RoutineError> {
		match (self.function, constants.get(2).copied().flatten()) {
			(TileFunction::Paint, Some(tiles)) => {
				let prepared: Prepared = Arc::new(group_by_tile(decode_edits(ctx, tiles)?));
				Ok(Some(prepared))
			}
			_ => Ok(None),
		}
	}

	fn execute_prepared(
		&self,
		ctx: &mut FunctionContext<'_>,
		args: &[(FieldRef, ArrayRef)],
		prepared: Option<&Prepared>,
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		match prepared.and_then(|prepared| prepared.downcast_ref::<EditsByTile>()) {
			Some(by_tile) => {
				check_lengths(ctx, args)?;
				paint_rows(ctx, by_tile, args)
			}
			None => self.call(ctx, args),
		}
	}
}

fn edit_list_type() -> ValueType {
	ValueType::List(Box::new(ValueType::Blob))
}

fn first_value(arg: &(FieldRef, ArrayRef)) -> Result<Value, RoutineError> {
	Ok(ColumnView::try_from(arg)?.get_value(0))
}

fn edit_list_rows(ctx: &FunctionContext<'_>, edits: Vec<Vec<u8>>) -> Result<(FieldRef, ArrayRef), RoutineError> {
	let list = Value::List(edits.into_iter().map(|edit| Value::Blob(Blob::new(edit))).collect());
	Ok(any_typed(ctx.fragment.text(), (0..ctx.row_count).map(|_| list.clone()), edit_list_type()))
}

fn texts(value: &Value) -> Result<Vec<String>, String> {
	match value {
		Value::List(items) => items.iter().map(text).collect(),
		Value::Any(inner) => texts(inner),
		other => Err(format!("expected a list of text, got {:?}", other.get_type())),
	}
}

fn text(value: &Value) -> Result<String, String> {
	match value {
		Value::Utf8(text) => Ok(text.clone()),
		Value::Any(inner) => text(inner),
		other => Err(format!("expected text, got {:?}", other.get_type())),
	}
}

fn coordinate(value: &Value) -> Result<u32, String> {
	match value {
		Value::Uint2(v) => Ok(u32::from(*v)),
		Value::Any(inner) => coordinate(inner),
		other => Err(format!("expected a uint2 coordinate, got {:?}", other.get_type())),
	}
}

fn parse_pixels(items: &[String]) -> Result<Vec<Vec<u8>>, String> {
	let mut seen: HashMap<(u32, u32), usize> = HashMap::new();
	let mut pixels = Vec::with_capacity(items.len());
	for (index, item) in items.iter().enumerate() {
		let n = index + 1;
		let malformed = || format!("pixel {n} \"{item}\" must be \"x,y,color\"");
		let parts: Vec<&str> = item.split(',').map(str::trim).collect();
		let [x, y, color] = parts.as_slice() else {
			return Err(malformed());
		};
		let (Ok(x), Ok(y), Ok(color)) = (x.parse::<u32>(), y.parse::<u32>(), color.parse::<u32>()) else {
			return Err(malformed());
		};
		if x >= CANVAS || y >= CANVAS {
			return Err(format!("pixel {n} at {x},{y} is outside the {CANVAS}x{CANVAS} canvas"));
		}
		if color >= COLORS as u32 {
			return Err(format!("pixel {n} color {color} is outside the palette 0..{}", COLORS - 1));
		}
		if let Some(first) = seen.insert((x, y), n) {
			return Err(format!("pixel {x},{y} is given twice (items {first} and {n})"));
		}
		pixels.push((x, y, color as u8));
	}
	Ok(tile_edits(pixels))
}

fn parse_sprite(x: u32, y: u32, rows: &[String], legend: &[String]) -> Result<Vec<Vec<u8>>, String> {
	let mut colors: HashMap<char, (u8, usize)> = HashMap::new();
	for (index, entry) in legend.iter().enumerate() {
		let n = index + 1;
		let malformed = || format!("legend {n} \"{entry}\" must be \"letter=color\"");
		let (key, color) = entry.split_once('=').ok_or_else(malformed)?;
		let mut chars = key.chars();
		let (Some(letter), None) = (chars.next(), chars.next()) else {
			return Err(malformed());
		};
		if letter == '.' {
			return Err(format!("legend {n} \".\" is reserved for \"leave the pixel as it is\""));
		}
		let color: u32 = color.parse().map_err(|_| malformed())?;
		if color >= COLORS as u32 {
			return Err(format!("legend {n} color {color} is outside the palette 0..{}", COLORS - 1));
		}
		if let Some((_, first)) = colors.insert(letter, (color as u8, n)) {
			return Err(format!("legend letter \"{letter}\" is given twice (entries {first} and {n})"));
		}
	}
	let width = rows.first().map_or(0, |row| row.chars().count());
	let height = rows.len();
	for (index, row) in rows.iter().enumerate() {
		let cells = row.chars().count();
		if cells != width {
			return Err(format!(
				"row {} has {cells} cells, row 1 has {width}; all rows must be the same length",
				index + 1
			));
		}
	}
	if width > MAX_SPRITE || height > MAX_SPRITE {
		return Err(format!("sprite is {width}x{height}, max is {MAX_SPRITE}x{MAX_SPRITE}"));
	}
	if width > 0 && height > 0 {
		let (end_x, end_y) = (x + width as u32 - 1, y + height as u32 - 1);
		if end_x >= CANVAS || end_y >= CANVAS {
			return Err(format!(
				"sprite {width}x{height} at {x},{y} ends at {end_x},{end_y}, past the canvas edge {}",
				CANVAS - 1
			));
		}
	}
	let mut pixels = Vec::new();
	for (r, row) in rows.iter().enumerate() {
		for (c, letter) in row.chars().enumerate() {
			if letter == '.' {
				continue;
			}
			let (color, _) = colors.get(&letter).ok_or_else(|| {
				format!("row {} column {} letter \"{letter}\" is not in the legend", r + 1, c + 1)
			})?;
			pixels.push((x + c as u32, y + r as u32, *color));
		}
	}
	Ok(tile_edits(pixels))
}

fn tile_edits(pixels: Vec<(u32, u32, u8)>) -> Vec<Vec<u8>> {
	let mut by_tile: BTreeMap<(u8, u8), Vec<(usize, u8)>> = BTreeMap::new();
	for (x, y, color) in pixels {
		let (x, y) = (x as usize, y as usize);
		let tile = ((x / TILE) as u8, (y / TILE) as u8);
		by_tile.entry(tile).or_default().push(((y % TILE) * TILE + x % TILE, color));
	}
	by_tile.into_iter().map(|((tx, ty), pixels)| encode_edit(tx, ty, &pixels)).collect()
}

fn edit_list(value: &Value) -> Result<Vec<Vec<u8>>, String> {
	let items = match value {
		Value::List(items) => items,
		Value::Any(inner) => return edit_list(inner),
		other => return Err(format!("expected a list of tile edits, got {:?}", other.get_type())),
	};
	items.iter()
		.map(|item| match item {
			Value::Blob(edit) => Ok(edit.as_bytes().to_vec()),
			other => Err(format!("expected a tile edit blob, got {:?}", other.get_type())),
		})
		.collect()
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
			"from canvas::state filter { key == \"canvas\" } aggregate { n: math::count(key) }",
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
	db.command_as_root("insert canvas::state [{ key: \"canvas\", ready: true }]", Params::None)
		.map(|_| ())
		.map_err(|e| format!("mark canvas ready: {e:?}"))
}

fn tile_stored(db: &Database, tx: u8, ty: u8) -> Result<bool, String> {
	let frames = db
		.query_as_root(
			"from canvas::tile filter { tile == $tile } map { tile, pixels }",
			named(&[("tile", Value::Uint2(tile_key(tx, ty)))]),
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
	let rows =
		columns.iter().map(|&tx| named(&[("tile", Value::Uint2(tile_key(tx, ty))), ("pixels", white.clone())]));
	let mut b = db.engine().bulk_insert(IdentityId::root());
	b.table("canvas::tile").rows(rows).done();
	b.execute().map(|_| ()).map_err(|e| format!("seed tile row {ty}: {e:?}"))
}
