// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod any;
mod fixed;
mod varlen;

use std::{str, sync::Arc};

use arrow_array::{ArrayRef, LargeStringArray, RecordBatch, RecordBatchOptions, make_array};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow_schema::{FieldRef, Schema};
use reifydb_value::{
	encoding::LeBytes,
	reifydb_assertions,
	value::{
		container::varlen_array::blob_array,
		diff_type::DiffType,
		frame::frame::Frame,
		value_type::{
			ValueType,
			field::{FieldType, to_field},
		},
	},
};

use crate::{
	error::DecodeError,
	frame::{
		encoding::dict::{decode_dict_blob, decode_dict_utf8},
		format::{
			COL_FLAG_HAS_NONES, COLUMN_DESCRIPTOR_SIZE, Encoding, FRAME_HEADER_SIZE, MESSAGE_HEADER_SIZE,
			RBCF_MAGIC, RBCF_VERSION, dict_index_width_from_flags,
		},
	},
	tag::{TypeTag, ValueKind},
};

pub fn decode_frames(data: &[u8]) -> Result<Vec<Frame>, DecodeError> {
	let mut pos = 0;

	check_len(data, pos, MESSAGE_HEADER_SIZE)?;
	let magic = read_u32(data, pos);
	pos += 4;
	if magic != RBCF_MAGIC {
		return Err(DecodeError::InvalidMagic(magic));
	}
	let version = read_u16(data, pos);
	pos += 2;
	if version != RBCF_VERSION {
		return Err(DecodeError::UnsupportedVersion(version));
	}
	let _flags = read_u16(data, pos);
	pos += 2;
	let frame_count = read_u32(data, pos) as usize;
	pos += 4;
	let _total_size = read_u32(data, pos) as usize;
	pos += 4;

	let mut frames = Vec::with_capacity(frame_count);
	for _ in 0..frame_count {
		let (frame, new_pos) = decode_frame(data, pos)?;
		frames.push(frame);
		pos = new_pos;
	}

	reifydb_assertions! {
		assert!(
			pos == _total_size,
			"the RBCF message header declared a total size that disagrees with the bytes consumed while decoding, so the message is truncated or carries trailing bytes and a peer would mis-frame the following message (declared={} consumed={})",
			_total_size,
			pos
		);
	}

	Ok(frames)
}

struct FrameHeader {
	row_count: usize,
	column_count: usize,
	op: Option<DiffType>,
}

fn decode_frame(data: &[u8], start: usize) -> Result<(Frame, usize), DecodeError> {
	let (header, pos) = read_frame_header(data, start)?;
	let (columns, pos) = read_frame_columns(data, pos, header.column_count)?;

	Ok((
		Frame {
			batch: frame_batch(columns, header.row_count)?,
			op: header.op,
		},
		pos,
	))
}

pub(crate) fn frame_batch(columns: Vec<(FieldRef, ArrayRef)>, row_count: usize) -> Result<RecordBatch, DecodeError> {
	let (fields, arrays): (Vec<FieldRef>, Vec<ArrayRef>) = columns.into_iter().unzip();
	RecordBatch::try_new_with_options(
		Arc::new(Schema::new(fields)),
		arrays,
		&RecordBatchOptions::new().with_row_count(Some(row_count)),
	)
	.map_err(|error| DecodeError::InvalidData(format!("frame columns do not form a batch: {error}")))
}

#[inline]
fn read_frame_header(data: &[u8], start: usize) -> Result<(FrameHeader, usize), DecodeError> {
	let mut pos = start;
	check_len(data, pos, FRAME_HEADER_SIZE)?;
	let row_count = read_u32(data, pos) as usize;
	pos += 4;
	let column_count = read_u16(data, pos) as usize;
	pos += 2;
	let meta_flags = data[pos];
	if meta_flags != 0 {
		return Err(DecodeError::InvalidData(format!("frame meta byte must be 0, found 0x{meta_flags:02X}")));
	}
	pos += 1;
	let op = match data[pos] {
		0 => None,
		raw => Some(DiffType::from_u8(raw)
			.ok_or_else(|| DecodeError::InvalidData(format!("unknown frame op {raw}")))?),
	};
	pos += 1;
	let _frame_size = read_u32(data, pos);
	pos += 4;
	Ok((
		FrameHeader {
			row_count,
			column_count,
			op,
		},
		pos,
	))
}

#[inline]
fn read_frame_columns(
	data: &[u8],
	mut pos: usize,
	column_count: usize,
) -> Result<(Vec<(FieldRef, ArrayRef)>, usize), DecodeError> {
	let mut columns = Vec::with_capacity(column_count);
	for _ in 0..column_count {
		let (col, new_pos) = decode_column(data, pos)?;
		columns.push(col);
		pos = new_pos;
	}
	Ok((columns, pos))
}

fn decode_column(data: &[u8], start: usize) -> Result<((FieldRef, ArrayRef), usize), DecodeError> {
	let mut pos = start;
	check_len(data, pos, COLUMN_DESCRIPTOR_SIZE)?;

	let type_code = data[pos];
	pos += 1;
	let encoding_byte = data[pos];
	pos += 1;
	let flags = data[pos];
	pos += 1;
	let _reserved = data[pos];
	pos += 1;
	let name_len = read_u16(data, pos) as usize;
	pos += 2;
	let _reserved2 = read_u16(data, pos);
	pos += 2;
	let row_count = read_u32(data, pos) as usize;
	pos += 4;
	let nones_len = read_u32(data, pos) as usize;
	pos += 4;
	let data_len = read_u32(data, pos) as usize;
	pos += 4;
	let offsets_len = read_u32(data, pos) as usize;
	pos += 4;
	let extra_len = read_u32(data, pos) as usize;
	pos += 4;

	let encoding = Encoding::from_u8(encoding_byte).ok_or(DecodeError::UnknownEncoding(encoding_byte))?;
	let has_nones = flags & COL_FLAG_HAS_NONES != 0;
	let tag = TypeTag::from_byte(type_code)?;
	let depth = tag.depth() as usize;
	let base_code = tag.kind_bits();

	check_len(data, pos, name_len)?;
	let name = str::from_utf8(&data[pos..pos + name_len])
		.map_err(|e| DecodeError::InvalidData(format!("invalid column name: {}", e)))?
		.to_string();
	pos += name_len;
	let name_pad = (4 - (name_len % 4)) % 4;
	pos += name_pad;

	let result = (|| -> Result<((FieldRef, ArrayRef), usize), DecodeError> {
		let mut pos = pos;

		if has_nones != (depth > 0) {
			return Err(DecodeError::InvalidData(format!(
				"column type code 0x{type_code:02X} has option depth {depth} but the has-nones flag is {}",
				if has_nones {
					"set"
				} else {
					"clear"
				}
			)));
		}
		if depth > 1 {
			return Err(DecodeError::InvalidData(format!(
				"column type code 0x{type_code:02X} has option depth {depth}, but a column holds at most one option layer"
			)));
		}
		let layer_len = row_count.div_ceil(8);
		if nones_len != depth * layer_len {
			return Err(DecodeError::InvalidData(format!(
				"nones length {nones_len} disagrees with option depth {depth} and row count {row_count} (expected {})",
				depth * layer_len
			)));
		}
		check_len(data, pos, nones_len)?;
		let layers: Vec<BooleanBuffer> = (0..depth)
			.map(|layer| {
				let start = pos + layer * layer_len;
				decode_bitvec(&data[start..start + layer_len], row_count)
			})
			.collect();
		pos += nones_len;

		check_len(data, pos, data_len)?;
		let data_bytes = &data[pos..pos + data_len];
		pos += data_len;

		check_len(data, pos, offsets_len)?;
		let offsets_bytes = &data[pos..pos + offsets_len];
		pos += offsets_len;

		check_len(data, pos, extra_len)?;
		let extra_bytes = &data[pos..pos + extra_len];
		pos += extra_len;

		let (value_type, array) = decode_column_dispatch(
			base_code,
			encoding,
			flags,
			row_count,
			data_bytes,
			offsets_bytes,
			extra_bytes,
			layers.last(),
		)?;

		Ok((decoded_column(&name, value_type, array, layers.into_iter().next())?, pos))
	})()
	.map_err(|e| DecodeError::ColumnDecodeFailed {
		column_name: name.clone(),
		row_index: None,
		source: Box::new(e),
	})?;

	Ok(result)
}

pub(crate) fn decoded_column(
	name: &str,
	value_type: ValueType,
	array: ArrayRef,
	layer: Option<BooleanBuffer>,
) -> Result<(FieldRef, ArrayRef), DecodeError> {
	let declared_type = matches!(value_type, ValueType::List(_) | ValueType::Record(_) | ValueType::Tuple(_))
		.then(|| value_type.clone());
	let (value_type, array) = match layer {
		Some(layer) => (ValueType::Option(Box::new(value_type)), attach_layer(array, layer)?),
		None if array.logical_null_count() == 0 => (value_type, array),
		None if matches!(value_type, ValueType::Digest { .. }) => {
			(ValueType::Option(Box::new(value_type)), array)
		}
		None => {
			return Err(DecodeError::InvalidData(format!(
				"column of type {value_type:?} holds {} none cells but has no option layer",
				array.logical_null_count()
			)));
		}
	};
	let field_type = FieldType {
		value_type: Some(value_type),
		declared_type,
		..FieldType::default()
	};
	Ok((Arc::new(to_field(name, &field_type)), array))
}

fn attach_layer(array: ArrayRef, layer: BooleanBuffer) -> Result<ArrayRef, DecodeError> {
	let nulls = NullBuffer::union(Some(&NullBuffer::new(layer)), array.logical_nulls().as_ref());
	let data =
		array.to_data().into_builder().nulls(nulls).build().map_err(|error| {
			DecodeError::InvalidData(format!("option layer does not fit the column: {error}"))
		})?;
	Ok(make_array(data))
}

pub(crate) fn column_type_from_code(type_code: u8) -> Result<ValueType, DecodeError> {
	TypeTag::from_byte(type_code)?.to_type()
}

#[allow(clippy::too_many_arguments)]
fn decode_column_dispatch(
	type_code: u8,
	encoding: Encoding,
	flags: u8,
	row_count: usize,
	data: &[u8],
	offsets: &[u8],
	extra: &[u8],
	defined: Option<&BooleanBuffer>,
) -> Result<(ValueType, ArrayRef), DecodeError> {
	if type_code == ValueKind::Digest.byte() {
		if encoding != Encoding::Plain {
			return Err(DecodeError::InvalidData(format!(
				"digest column must use plain encoding, found {encoding:?}"
			)));
		}
		return varlen::decode_digest_plain(row_count, data, offsets, extra);
	}
	if let Some(kind @ ValueKind::Decimal) = ValueKind::from_byte(type_code) {
		return fixed::decode_unscaled_column(kind, encoding, row_count, data, extra);
	}
	let ty = column_type_from_code(type_code)?;

	let array: ArrayRef = match encoding {
		Encoding::Plain | Encoding::BitPack => {
			if ty == ValueType::Any {
				any::decode_any_column(row_count, data, defined)?
			} else if let Some(result) = fixed::decode_fixed_plain(type_code, row_count, data) {
				result?
			} else if let Some(result) = varlen::decode_varlen_plain(type_code, row_count, data, offsets) {
				result?
			} else {
				return Err(DecodeError::UnsupportedType(format!("{:?}", ty)));
			}
		}
		Encoding::Dict => match ty {
			ValueType::Utf8 => {
				let index_width = dict_index_width_from_flags(flags);
				let strings = decode_dict_utf8(data, extra, row_count, index_width)?;
				Arc::new(LargeStringArray::from(strings))
			}
			ValueType::Blob => {
				let index_width = dict_index_width_from_flags(flags);
				let blobs = decode_dict_blob(data, extra, row_count, index_width)?;
				Arc::new(blob_array(&blobs))
			}
			_ => {
				return Err(DecodeError::InvalidData(format!(
					"Dict encoding not supported for type {:?}",
					ty
				)));
			}
		},
		Encoding::Rle => fixed::decode_rle_column(type_code, row_count, data)?,
		Encoding::Delta => fixed::decode_delta_column(type_code, row_count, data)?,
		Encoding::DeltaRle => fixed::decode_delta_rle_column(type_code, row_count, data)?,
	};
	Ok((ty, array))
}

fn decode_bitvec(data: &[u8], len: usize) -> BooleanBuffer {
	BooleanBuffer::new(Buffer::from_vec(data.to_vec()), 0, len)
}

#[inline]
fn read_u16(data: &[u8], pos: usize) -> u16 {
	u16::read_le(&data[pos..])
}

#[inline]
fn read_u32(data: &[u8], pos: usize) -> u32 {
	u32::read_le(&data[pos..])
}

fn check_len(data: &[u8], pos: usize, needed: usize) -> Result<(), DecodeError> {
	if pos + needed > data.len() {
		Err(DecodeError::UnexpectedEof {
			expected: needed,
			available: data.len().saturating_sub(pos),
		})
	} else {
		Ok(())
	}
}
