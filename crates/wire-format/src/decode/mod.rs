// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! RBCF message decoder: &[u8] -> Vec<Frame>.

mod any;
mod fixed;
mod varlen;

use std::str;

use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use reifydb_type::{
	util::bitvec::BitVec,
	value::{
		container::{blob::BlobContainer, number::NumberContainer, utf8::Utf8Container},
		datetime::DateTime,
		decimal::Decimal,
		frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
		int::Int,
		row_number::RowNumber,
		r#type::Type,
		uint::Uint,
	},
};

use crate::{
	encoding::dict::{decode_dict_blob, decode_dict_table_bytes, decode_dict_utf8, read_index},
	error::DecodeError,
	format::{
		COL_FLAG_HAS_NONES, COLUMN_DESCRIPTOR_SIZE, Encoding, FRAME_HEADER_SIZE, MESSAGE_HEADER_SIZE,
		META_HAS_CREATED_AT, META_HAS_ROW_NUMBERS, META_HAS_UPDATED_AT, RBCF_MAGIC, RBCF_VERSION,
		dict_index_width_from_flags,
	},
};

/// Decode RBCF binary data into frames.
pub fn decode_frames(data: &[u8]) -> Result<Vec<Frame>, DecodeError> {
	let mut pos = 0;

	// Read message header
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
	let _total_size = read_u32(data, pos);
	pos += 4;

	let mut frames = Vec::with_capacity(frame_count);
	for _ in 0..frame_count {
		let (frame, new_pos) = decode_frame(data, pos)?;
		frames.push(frame);
		pos = new_pos;
	}

	Ok(frames)
}

fn decode_frame(data: &[u8], start: usize) -> Result<(Frame, usize), DecodeError> {
	let mut pos = start;
	check_len(data, pos, FRAME_HEADER_SIZE)?;

	let row_count = read_u32(data, pos) as usize;
	pos += 4;
	let column_count = read_u16(data, pos) as usize;
	pos += 2;
	let meta_flags = data[pos];
	pos += 1;
	let _reserved = data[pos];
	pos += 1;
	let _frame_size = read_u32(data, pos);
	pos += 4;

	// Read metadata arrays
	let mut row_numbers = Vec::new();
	if meta_flags & META_HAS_ROW_NUMBERS != 0 {
		check_len(data, pos, row_count * 8)?;
		row_numbers.reserve(row_count);
		for _ in 0..row_count {
			let v = read_u64(data, pos);
			pos += 8;
			row_numbers.push(RowNumber::new(v));
		}
	}

	let mut created_at = Vec::new();
	if meta_flags & META_HAS_CREATED_AT != 0 {
		check_len(data, pos, row_count * 8)?;
		created_at.reserve(row_count);
		for _ in 0..row_count {
			let v = read_u64(data, pos);
			pos += 8;
			created_at.push(DateTime::from_nanos(v));
		}
	}

	let mut updated_at = Vec::new();
	if meta_flags & META_HAS_UPDATED_AT != 0 {
		check_len(data, pos, row_count * 8)?;
		updated_at.reserve(row_count);
		for _ in 0..row_count {
			let v = read_u64(data, pos);
			pos += 8;
			updated_at.push(DateTime::from_nanos(v));
		}
	}

	// Read columns
	let mut columns = Vec::with_capacity(column_count);
	for _ in 0..column_count {
		let (col, new_pos) = decode_column(data, pos)?;
		columns.push(col);
		pos = new_pos;
	}

	Ok((
		Frame {
			row_numbers,
			created_at,
			updated_at,
			columns,
		},
		pos,
	))
}

fn decode_column(data: &[u8], start: usize) -> Result<(FrameColumn, usize), DecodeError> {
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

	// Read column name
	check_len(data, pos, name_len)?;
	let name = str::from_utf8(&data[pos..pos + name_len])
		.map_err(|e| DecodeError::InvalidData(format!("invalid column name: {}", e)))?
		.to_string();
	pos += name_len;
	let name_pad = (4 - (name_len % 4)) % 4;
	pos += name_pad;

	// Wrap all remaining reads and dispatch in a closure to add column context on error
	let result = (|| -> Result<(FrameColumnData, usize), DecodeError> {
		let mut pos = pos;

		// Read nones bitmap
		let nones = if has_nones && nones_len > 0 {
			check_len(data, pos, nones_len)?;
			let bv = decode_bitvec(&data[pos..pos + nones_len], row_count);
			pos += nones_len;
			Some(bv)
		} else {
			pos += nones_len;
			None
		};

		// Read data segment
		check_len(data, pos, data_len)?;
		let data_bytes = &data[pos..pos + data_len];
		pos += data_len;

		// Read offsets segment
		check_len(data, pos, offsets_len)?;
		let offsets_bytes = &data[pos..pos + offsets_len];
		pos += offsets_len;

		// Read extra segment (for dict encoding)
		check_len(data, pos, extra_len)?;
		let extra_bytes = &data[pos..pos + extra_len];
		pos += extra_len;

		// Decode column data based on type code and encoding
		let col_data = decode_column_dispatch(
			type_code,
			encoding,
			flags,
			row_count,
			data_bytes,
			offsets_bytes,
			extra_bytes,
		)?;

		// Wrap in Option if has nones
		let col_data = if let Some(bitvec) = nones {
			FrameColumnData::Option {
				inner: Box::new(col_data),
				bitvec,
			}
		} else {
			col_data
		};

		Ok((col_data, pos))
	})()
	.map_err(|e| DecodeError::ColumnDecodeFailed {
		column_name: name.clone(),
		row_index: None,
		source: Box::new(e),
	})?;

	let (col_data, pos) = result;
	Ok((
		FrameColumn {
			name,
			data: col_data,
		},
		pos,
	))
}

fn decode_column_dispatch(
	type_code: u8,
	encoding: Encoding,
	flags: u8,
	row_count: usize,
	data: &[u8],
	offsets: &[u8],
	extra: &[u8],
) -> Result<FrameColumnData, DecodeError> {
	// Strip the option bit - the option-ness is represented separately via the nones bitmap
	// and handled in the caller; the encoding strategies only know about the inner concrete type.
	let type_code = type_code & 0x7F;
	let ty = Type::from_u8(type_code);

	match encoding {
		Encoding::Plain | Encoding::BitPack => {
			// Try Any first (special handling)
			if ty == Type::Any {
				return any::decode_any_column(row_count, data);
			}
			// Try fixed-width types
			if let Some(result) = fixed::decode_fixed_plain(type_code, row_count, data) {
				return result;
			}
			// Try variable-length types
			if let Some(result) = varlen::decode_varlen_plain(type_code, row_count, data, offsets) {
				return result;
			}
			Err(DecodeError::UnsupportedType(format!("{:?}", ty)))
		}
		Encoding::Dict => match ty {
			Type::Utf8 => {
				let index_width = dict_index_width_from_flags(flags);
				let strings = decode_dict_utf8(data, extra, row_count, index_width)?;
				Ok(FrameColumnData::Utf8(Utf8Container::new(strings)))
			}
			Type::Blob => {
				let index_width = dict_index_width_from_flags(flags);
				let blobs = decode_dict_blob(data, extra, row_count, index_width)?;
				Ok(FrameColumnData::Blob(BlobContainer::new(blobs)))
			}
			Type::Int => {
				let index_width = dict_index_width_from_flags(flags);
				let dict_entries = decode_dict_table_bytes(extra)?;
				let mut values = Vec::with_capacity(row_count);
				for i in 0..row_count {
					let idx = read_index(data, i, index_width) as usize;
					if idx >= dict_entries.len() {
						return Err(DecodeError::InvalidData(format!(
							"dict index {} out of range (dict has {} entries)",
							idx,
							dict_entries.len()
						)));
					}
					let big = BigInt::from_signed_bytes_le(&dict_entries[idx]);
					values.push(Int(big));
				}
				Ok(FrameColumnData::Int(NumberContainer::new(values)))
			}
			Type::Uint => {
				let index_width = dict_index_width_from_flags(flags);
				let dict_entries = decode_dict_table_bytes(extra)?;
				let mut values = Vec::with_capacity(row_count);
				for i in 0..row_count {
					let idx = read_index(data, i, index_width) as usize;
					if idx >= dict_entries.len() {
						return Err(DecodeError::InvalidData(format!(
							"dict index {} out of range (dict has {} entries)",
							idx,
							dict_entries.len()
						)));
					}
					let big = BigInt::from_signed_bytes_le(&dict_entries[idx]);
					values.push(Uint(big));
				}
				Ok(FrameColumnData::Uint(NumberContainer::new(values)))
			}
			Type::Decimal => {
				let index_width = dict_index_width_from_flags(flags);
				let dict_entries = decode_dict_table_bytes(extra)?;
				let mut values = Vec::with_capacity(row_count);
				for i in 0..row_count {
					let idx = read_index(data, i, index_width) as usize;
					if idx >= dict_entries.len() {
						return Err(DecodeError::InvalidData(format!(
							"dict index {} out of range (dict has {} entries)",
							idx,
							dict_entries.len()
						)));
					}
					let s = str::from_utf8(&dict_entries[idx]).map_err(|e| {
						DecodeError::InvalidData(format!("invalid decimal string: {}", e))
					})?;
					let dec: BigDecimal = s.parse().map_err(|e| {
						DecodeError::InvalidData(format!("invalid decimal: {}", e))
					})?;
					values.push(Decimal::new(dec));
				}
				Ok(FrameColumnData::Decimal(NumberContainer::new(values)))
			}
			_ => Err(DecodeError::InvalidData(format!("Dict encoding not supported for type {:?}", ty))),
		},
		Encoding::Rle => {
			// Try fixed-width RLE first, then variable-length RLE
			match ty {
				Type::Int | Type::Uint | Type::Decimal => {
					varlen::decode_rle_varlen_column(type_code, row_count, data)
				}
				_ => fixed::decode_rle_column(type_code, row_count, data),
			}
		}
		Encoding::Delta => fixed::decode_delta_column(type_code, row_count, data),
		Encoding::DeltaRle => fixed::decode_delta_rle_column(type_code, row_count, data),
	}
}

fn decode_bitvec(data: &[u8], len: usize) -> BitVec {
	BitVec::from_raw(data.to_vec(), len)
}

#[inline]
fn read_u16(data: &[u8], pos: usize) -> u16 {
	u16::from_le_bytes([data[pos], data[pos + 1]])
}

#[inline]
fn read_u32(data: &[u8], pos: usize) -> u32 {
	u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
}

#[inline]
fn read_u64(data: &[u8], pos: usize) -> u64 {
	u64::from_le_bytes([
		data[pos],
		data[pos + 1],
		data[pos + 2],
		data[pos + 3],
		data[pos + 4],
		data[pos + 5],
		data[pos + 6],
		data[pos + 7],
	])
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
