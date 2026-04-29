// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! PLAIN encoding: raw little-endian values for fixed-size types,
//! offset array + concatenated data for variable-length types,
//! bit-packed for booleans.

use std::iter;

use reifydb_type::{
	util::bitvec::BitVec,
	value::{
		Value,
		container::{blob::BlobContainer, dictionary::DictionaryContainer, utf8::Utf8Container},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		dictionary::DictionaryEntryId,
		duration::Duration,
		frame::data::FrameColumnData,
		identity::IdentityId,
		int::Int,
		time::Time,
		r#type::Type,
		uint::Uint,
		uuid::{Uuid4, Uuid7},
	},
};

use crate::{encode::any::encode_any_value, error::EncodeError};

/// Helper macro for encoding fixed-size numeric types via to_le_bytes.
macro_rules! encode_fixed {
	($container:expr, $ty:expr, $elem_size:expr) => {{
		let slice = &**$container;
		let mut buf = Vec::with_capacity(slice.len() * $elem_size);
		for v in slice {
			buf.extend_from_slice(&v.to_le_bytes());
		}
		PlainEncoded {
			data: buf,
			offsets: vec![],
			nones: vec![],
			type_code: $ty.to_u8(),
			has_nones: false,
		}
	}};
}

/// Result of encoding a column's data in PLAIN format.
pub struct PlainEncoded {
	/// The encoded data bytes.
	pub data: Vec<u8>,
	/// Offset array bytes (for variable-length types), empty otherwise.
	pub offsets: Vec<u8>,
	/// Nones bitmap bytes (for Option columns), empty otherwise.
	pub nones: Vec<u8>,
	/// The base type code.
	pub type_code: u8,
	/// Whether this column has nones (is an Option type).
	pub has_nones: bool,
}

/// Encode a FrameColumnData into PLAIN format.
pub fn encode_plain(col: &FrameColumnData) -> Result<PlainEncoded, EncodeError> {
	match col {
		FrameColumnData::Option {
			inner,
			bitvec,
		} => {
			let mut result = encode_plain_inner(inner)?;
			result.nones = encode_bitvec(bitvec);
			result.has_nones = true;
			Ok(result)
		}
		other => encode_plain_inner(other),
	}
}

fn encode_plain_inner(col: &FrameColumnData) -> Result<PlainEncoded, EncodeError> {
	let result = match col {
		FrameColumnData::Bool(c) => {
			let bv: &BitVec = c;
			PlainEncoded {
				data: encode_bitvec(bv),
				offsets: vec![],
				nones: vec![],
				type_code: Type::Boolean.to_u8(),
				has_nones: false,
			}
		}
		FrameColumnData::Float4(c) => encode_fixed!(c, Type::Float4, 4),
		FrameColumnData::Float8(c) => encode_fixed!(c, Type::Float8, 8),
		FrameColumnData::Int1(c) => encode_fixed!(c, Type::Int1, 1),
		FrameColumnData::Int2(c) => encode_fixed!(c, Type::Int2, 2),
		FrameColumnData::Int4(c) => encode_fixed!(c, Type::Int4, 4),
		FrameColumnData::Int8(c) => encode_fixed!(c, Type::Int8, 8),
		FrameColumnData::Int16(c) => encode_fixed!(c, Type::Int16, 16),
		FrameColumnData::Uint1(c) => encode_fixed!(c, Type::Uint1, 1),
		FrameColumnData::Uint2(c) => encode_fixed!(c, Type::Uint2, 2),
		FrameColumnData::Uint4(c) => encode_fixed!(c, Type::Uint4, 4),
		FrameColumnData::Uint8(c) => encode_fixed!(c, Type::Uint8, 8),
		FrameColumnData::Uint16(c) => encode_fixed!(c, Type::Uint16, 16),
		FrameColumnData::Date(c) => {
			let slice: &[Date] = c;
			let mut buf = Vec::with_capacity(slice.len() * 4);
			for v in slice {
				buf.extend_from_slice(&v.to_days_since_epoch().to_le_bytes());
			}
			PlainEncoded {
				data: buf,
				offsets: vec![],
				nones: vec![],
				type_code: Type::Date.to_u8(),
				has_nones: false,
			}
		}
		FrameColumnData::DateTime(c) => {
			let slice: &[DateTime] = c;
			let mut buf = Vec::with_capacity(slice.len() * 8);
			for v in slice {
				buf.extend_from_slice(&v.to_nanos().to_le_bytes());
			}
			PlainEncoded {
				data: buf,
				offsets: vec![],
				nones: vec![],
				type_code: Type::DateTime.to_u8(),
				has_nones: false,
			}
		}
		FrameColumnData::Time(c) => {
			let slice: &[Time] = c;
			let mut buf = Vec::with_capacity(slice.len() * 8);
			for v in slice {
				buf.extend_from_slice(&v.to_nanos_since_midnight().to_le_bytes());
			}
			PlainEncoded {
				data: buf,
				offsets: vec![],
				nones: vec![],
				type_code: Type::Time.to_u8(),
				has_nones: false,
			}
		}
		FrameColumnData::Duration(c) => {
			let slice: &[Duration] = c;
			let mut buf = Vec::with_capacity(slice.len() * 16);
			for v in slice {
				buf.extend_from_slice(&v.get_months().to_le_bytes());
				buf.extend_from_slice(&v.get_days().to_le_bytes());
				buf.extend_from_slice(&v.get_nanos().to_le_bytes());
			}
			PlainEncoded {
				data: buf,
				offsets: vec![],
				nones: vec![],
				type_code: Type::Duration.to_u8(),
				has_nones: false,
			}
		}
		FrameColumnData::IdentityId(c) => {
			let slice: &[IdentityId] = c;
			let mut buf = Vec::with_capacity(slice.len() * 16);
			for v in slice {
				buf.extend_from_slice(v.0.0.as_bytes());
			}
			PlainEncoded {
				data: buf,
				offsets: vec![],
				nones: vec![],
				type_code: Type::IdentityId.to_u8(),
				has_nones: false,
			}
		}
		FrameColumnData::Uuid4(c) => {
			let slice: &[Uuid4] = c;
			let mut buf = Vec::with_capacity(slice.len() * 16);
			for v in slice {
				buf.extend_from_slice(v.0.as_bytes());
			}
			PlainEncoded {
				data: buf,
				offsets: vec![],
				nones: vec![],
				type_code: Type::Uuid4.to_u8(),
				has_nones: false,
			}
		}
		FrameColumnData::Uuid7(c) => {
			let slice: &[Uuid7] = c;
			let mut buf = Vec::with_capacity(slice.len() * 16);
			for v in slice {
				buf.extend_from_slice(v.0.as_bytes());
			}
			PlainEncoded {
				data: buf,
				offsets: vec![],
				nones: vec![],
				type_code: Type::Uuid7.to_u8(),
				has_nones: false,
			}
		}
		FrameColumnData::Utf8(c) => encode_varlen_strings(c, Type::Utf8),
		FrameColumnData::Blob(c) => encode_varlen_blobs(c, Type::Blob),
		FrameColumnData::Int(c) => {
			let slice: &[Int] = c;
			encode_varlen(slice.len(), |i| slice[i].0.to_signed_bytes_le(), Type::Int)
		}
		FrameColumnData::Uint(c) => {
			let slice: &[Uint] = c;
			encode_varlen(slice.len(), |i| slice[i].0.to_signed_bytes_le(), Type::Uint)
		}
		FrameColumnData::Decimal(c) => {
			let slice: &[Decimal] = c;
			encode_varlen(slice.len(), |i| slice[i].to_string().into_bytes(), Type::Decimal)
		}
		FrameColumnData::Any(c) => {
			let mut data = Vec::new();
			let none = Value::none();
			for i in 0..c.len() {
				let val = c.get(i).unwrap_or(&none);
				encode_any_value(val, &mut data)?;
			}
			return Ok(PlainEncoded {
				data,
				offsets: vec![],
				nones: vec![],
				type_code: Type::Any.to_u8(),
				has_nones: false,
			});
		}
		FrameColumnData::DictionaryId(c) => encode_dictionary_ids(c),
		FrameColumnData::Option {
			..
		} => unreachable!("Option handled in encode_plain"),
	};
	Ok(result)
}

/// Encode variable-length data using offset array + concatenated bytes.
fn encode_varlen(count: usize, get_bytes: impl Fn(usize) -> Vec<u8>, ty: Type) -> PlainEncoded {
	let mut offsets = Vec::with_capacity((count + 1) * 4);
	let mut data = Vec::new();
	let mut offset: u32 = 0;
	offsets.extend_from_slice(&offset.to_le_bytes());
	for i in 0..count {
		let bytes = get_bytes(i);
		data.extend_from_slice(&bytes);
		offset += bytes.len() as u32;
		offsets.extend_from_slice(&offset.to_le_bytes());
	}
	PlainEncoded {
		data,
		offsets,
		nones: vec![],
		type_code: ty.to_u8(),
		has_nones: false,
	}
}

fn encode_varlen_strings(c: &Utf8Container, ty: Type) -> PlainEncoded {
	encode_varlen(c.len(), |i| c.get(i).unwrap().as_bytes().to_vec(), ty)
}

fn encode_varlen_blobs(c: &BlobContainer, ty: Type) -> PlainEncoded {
	encode_varlen(c.len(), |i| c.get(i).unwrap_or(&[]).to_vec(), ty)
}

fn encode_dictionary_ids(c: &DictionaryContainer) -> PlainEncoded {
	let mut buf = Vec::new();
	if !c.is_empty() {
		let mut disc = 1u8;
		for i in 0..c.len() {
			if let Some(id) = c.get(i) {
				let d = match id {
					DictionaryEntryId::U1(_) => 1u8,
					DictionaryEntryId::U2(_) => 2u8,
					DictionaryEntryId::U4(_) => 4u8,
					DictionaryEntryId::U8(_) => 8u8,
					DictionaryEntryId::U16(_) => 16u8,
				};
				if d > disc {
					disc = d;
				}
			}
		}

		buf.push(disc);
		for i in 0..c.len() {
			if let Some(id) = c.get(i) {
				match disc {
					1 => buf.push(id.to_u128() as u8),
					2 => buf.extend_from_slice(&(id.to_u128() as u16).to_le_bytes()),
					4 => buf.extend_from_slice(&(id.to_u128() as u32).to_le_bytes()),
					8 => buf.extend_from_slice(&(id.to_u128() as u64).to_le_bytes()),
					16 => buf.extend_from_slice(&id.to_u128().to_le_bytes()),
					_ => unreachable!(),
				}
			} else {
				buf.extend(iter::repeat_n(0, disc as usize));
			}
		}
	}
	PlainEncoded {
		data: buf,
		offsets: vec![],
		nones: vec![],
		type_code: Type::DictionaryId.to_u8(),
		has_nones: false,
	}
}

/// Encode a BitVec as packed bytes (LSB-first, no length prefix).
pub fn encode_bitvec(bv: &BitVec) -> Vec<u8> {
	let len = bv.len();
	let byte_count = len.div_ceil(8);
	let mut buf = Vec::with_capacity(byte_count);
	for i in 0..byte_count {
		let mut byte = 0u8;
		for bit in 0..8 {
			let idx = i * 8 + bit;
			if idx < len && bv.get(idx) {
				byte |= 1 << bit;
			}
		}
		buf.push(byte);
	}
	buf
}
