// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::iter;

use arrow_array::{Array, FixedSizeBinaryArray, LargeBinaryArray, LargeStringArray};
use arrow_buffer::BooleanBuffer;
use reifydb_value::{
	encoding::LeBytes,
	value::{
		container::{
			any_array,
			decimal_array::{DecimalArray, u128s},
			dictionary_array, digest_array,
			temporal_array::{dates, datetimes, durations, times},
			uuid_array::{identity_ids, uuid4s, uuid7s},
		},
		date::Date,
		datetime::DateTime,
		dictionary::DictionaryEntryId,
		duration::Duration,
		frame::data::FrameColumnData,
		identity::IdentityId,
		time::Time,
		uuid::{Uuid4, Uuid7},
		value_type::ValueType,
	},
};

use crate::{error::EncodeError, frame::encode::any::encode_any_value, tag::ValueKind};

macro_rules! encode_fixed {
	($container:expr, $ty:expr, $elem:ty) => {
		encode_fixed!(slice: $container.values(), $ty, $elem)
	};
	(slice: $slice:expr, $ty:expr, $elem:ty) => {{
		let slice: &[$elem] = $slice;
		let mut buf = Vec::with_capacity(slice.len() * <$elem as LeBytes>::ENCODED_SIZE);
		for v in slice {
			buf.extend_from_slice(LeBytes::to_le_bytes(v).as_ref());
		}
		PlainEncoded {
			data: buf,
			offsets: vec![],
			type_code: ValueKind::of_type(&$ty).byte(),
		}
	}};
}

pub struct PlainEncoded {
	pub data: Vec<u8>,

	pub offsets: Vec<u8>,

	pub type_code: u8,
}

pub fn encode_plain(col: &FrameColumnData) -> Result<PlainEncoded, EncodeError> {
	let result = match col {
		FrameColumnData::Bool(c) => PlainEncoded {
			data: encode_bitvec(c.values()),
			offsets: vec![],
			type_code: ValueKind::Boolean.byte(),
		},
		FrameColumnData::Float4(c) => encode_fixed!(c, ValueType::Float4, f32),
		FrameColumnData::Float8(c) => encode_fixed!(c, ValueType::Float8, f64),
		FrameColumnData::Int1(c) => encode_fixed!(c, ValueType::Int1, i8),
		FrameColumnData::Int2(c) => encode_fixed!(c, ValueType::Int2, i16),
		FrameColumnData::Int4(c) => encode_fixed!(c, ValueType::Int4, i32),
		FrameColumnData::Int8(c) => encode_fixed!(c, ValueType::Int8, i64),
		FrameColumnData::Int16(c) => encode_fixed!(c, ValueType::Int16, i128),
		FrameColumnData::Uint1(c) => encode_fixed!(c, ValueType::Uint1, u8),
		FrameColumnData::Uint2(c) => encode_fixed!(c, ValueType::Uint2, u16),
		FrameColumnData::Uint4(c) => encode_fixed!(c, ValueType::Uint4, u32),
		FrameColumnData::Uint8(c) => encode_fixed!(c, ValueType::Uint8, u64),
		FrameColumnData::Uint16(c) => encode_fixed!(slice: &u128s(c), ValueType::Uint16, u128),
		FrameColumnData::Date(c) => {
			let slice: &[Date] = dates(c);
			let mut buf = Vec::with_capacity(slice.len() * Date::ENCODED_SIZE);
			for v in slice {
				buf.extend_from_slice(v.to_le_bytes().as_ref());
			}
			PlainEncoded {
				data: buf,
				offsets: vec![],
				type_code: ValueKind::Date.byte(),
			}
		}
		FrameColumnData::DateTime(c) => {
			let slice: &[DateTime] = datetimes(c);
			let mut buf = Vec::with_capacity(slice.len() * DateTime::ENCODED_SIZE);
			for v in slice {
				buf.extend_from_slice(v.to_le_bytes().as_ref());
			}
			PlainEncoded {
				data: buf,
				offsets: vec![],
				type_code: ValueKind::DateTime.byte(),
			}
		}
		FrameColumnData::Time(c) => {
			let slice: &[Time] = times(c);
			let mut buf = Vec::with_capacity(slice.len() * Time::ENCODED_SIZE);
			for v in slice {
				buf.extend_from_slice(v.to_le_bytes().as_ref());
			}
			PlainEncoded {
				data: buf,
				offsets: vec![],
				type_code: ValueKind::Time.byte(),
			}
		}
		FrameColumnData::Duration(c) => {
			let slice: &[Duration] = durations(c);
			let mut buf = Vec::with_capacity(slice.len() * Duration::ENCODED_SIZE);
			for v in slice {
				buf.extend_from_slice(v.to_le_bytes().as_ref());
			}
			PlainEncoded {
				data: buf,
				offsets: vec![],
				type_code: ValueKind::Duration.byte(),
			}
		}
		FrameColumnData::IdentityId(c) => {
			let slice: &[IdentityId] = identity_ids(c);
			let mut buf = Vec::with_capacity(slice.len() * IdentityId::ENCODED_SIZE);
			for v in slice {
				buf.extend_from_slice(v.to_le_bytes().as_ref());
			}
			PlainEncoded {
				data: buf,
				offsets: vec![],
				type_code: ValueKind::IdentityId.byte(),
			}
		}
		FrameColumnData::Uuid4(c) => {
			let slice: &[Uuid4] = uuid4s(c);
			let mut buf = Vec::with_capacity(slice.len() * Uuid4::ENCODED_SIZE);
			for v in slice {
				buf.extend_from_slice(v.to_le_bytes().as_ref());
			}
			PlainEncoded {
				data: buf,
				offsets: vec![],
				type_code: ValueKind::Uuid4.byte(),
			}
		}
		FrameColumnData::Uuid7(c) => {
			let slice: &[Uuid7] = uuid7s(c);
			let mut buf = Vec::with_capacity(slice.len() * Uuid7::ENCODED_SIZE);
			for v in slice {
				buf.extend_from_slice(v.to_le_bytes().as_ref());
			}
			PlainEncoded {
				data: buf,
				offsets: vec![],
				type_code: ValueKind::Uuid7.byte(),
			}
		}
		FrameColumnData::Utf8(c) => encode_varlen_strings(c, ValueType::Utf8),
		FrameColumnData::Blob(c) => encode_varlen_blobs(c, ValueType::Blob),
		FrameColumnData::Decimal(c) => encode_unscaled(c, ValueKind::Decimal),
		FrameColumnData::Any {
			container,
			..
		} => {
			let mut data = Vec::new();
			for val in any_array::values(container) {
				encode_any_value(&val, &mut data)?;
			}
			return Ok(PlainEncoded {
				data,
				offsets: vec![],
				type_code: ValueKind::Any.byte(),
			});
		}
		FrameColumnData::DictionaryId {
			container,
			..
		} => encode_dictionary_ids(container),
		FrameColumnData::Digest {
			container,
			..
		} => encode_varlen(
			container.len(),
			|i| match digest_array::get(container, i) {
				Some(digest) => digest.encode(),
				None => Vec::new(),
			},
			col.get_type(),
		),
		FrameColumnData::Option {
			..
		} => unreachable!("Option layers are stripped before plain encoding"),
	};
	Ok(result)
}

fn encode_unscaled(array: &DecimalArray, kind: ValueKind) -> PlainEncoded {
	let data = match array {
		DecimalArray::Decimal128(a) => a.values().iter().flat_map(|v| v.to_le_bytes()).collect(),
		DecimalArray::Decimal256(a) => a.values().iter().flat_map(|v| v.to_le_bytes()).collect(),
	};
	PlainEncoded {
		data,
		offsets: vec![],
		type_code: kind.byte(),
	}
}

fn encode_varlen(count: usize, get_bytes: impl Fn(usize) -> Vec<u8>, ty: ValueType) -> PlainEncoded {
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
		type_code: ValueKind::of_type(&ty).byte(),
	}
}

fn encode_varlen_strings(c: &LargeStringArray, ty: ValueType) -> PlainEncoded {
	encode_varlen(c.len(), |i| c.value(i).as_bytes().to_vec(), ty)
}

fn encode_varlen_blobs(c: &LargeBinaryArray, ty: ValueType) -> PlainEncoded {
	encode_varlen(c.len(), |i| c.value(i).to_vec(), ty)
}

fn encode_dictionary_ids(c: &FixedSizeBinaryArray) -> PlainEncoded {
	let mut buf = Vec::new();
	if !c.is_empty() {
		let mut disc = 1u8;
		for i in 0..c.len() {
			if let Some(id) = dictionary_array::get(c, i) {
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
			if let Some(id) = dictionary_array::get(c, i) {
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
		type_code: ValueKind::DictionaryId.byte(),
	}
}

pub fn encode_bitvec(bv: &BooleanBuffer) -> Vec<u8> {
	let len = bv.len();
	let byte_count = len.div_ceil(8);
	let mut buf = Vec::with_capacity(byte_count);
	for i in 0..byte_count {
		let mut byte = 0u8;
		for bit in 0..8 {
			let idx = i * 8 + bit;
			if idx < len && bv.value(idx) {
				byte |= 1 << bit;
			}
		}
		buf.push(byte);
	}
	buf
}
