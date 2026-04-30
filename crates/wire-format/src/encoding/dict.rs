// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, str};

use reifydb_type::value::{
	blob::Blob,
	container::{blob::BlobContainer, utf8::Utf8Container},
	r#type::Type,
};

use super::plain::PlainEncoded;
use crate::{error::DecodeError, format::dict_index_width_to_flags};

/// Result of dictionary encoding.
pub struct DictEncoded {
	/// Index array bytes.
	pub data: Vec<u8>,
	/// Dictionary table bytes.
	pub extra: Vec<u8>,
	/// Type code.
	pub type_code: u8,
	/// Column flags (includes dict index width bits).
	pub flags_bits: u8,
}

/// Try to dictionary-encode a Utf8 column. Returns None if not beneficial.
pub fn try_dict_encode_utf8(container: &Utf8Container, min_ratio: f64) -> Option<DictEncoded> {
	let row_count = container.len();
	if row_count == 0 {
		return None;
	}

	// Build dictionary
	let mut dict_map: HashMap<&str, u32> = HashMap::new();
	let mut dict_entries: Vec<&str> = Vec::new();

	for s in container.iter_str() {
		if !dict_map.contains_key(s) {
			let idx = dict_entries.len() as u32;
			dict_map.insert(s, idx);
			dict_entries.push(s);
		}
	}

	let dict_count = dict_entries.len();

	// Check cardinality ratio: dict is only worth it if distinct < row_count * min_ratio
	if dict_count as f64 >= row_count as f64 * min_ratio {
		return None;
	}

	// Determine index width
	let (index_width, flags_bits) = if dict_count <= 255 {
		(1usize, dict_index_width_to_flags(1))
	} else if dict_count <= 65535 {
		(2, dict_index_width_to_flags(2))
	} else {
		(4, dict_index_width_to_flags(4))
	};

	// Build index array
	let mut data = Vec::with_capacity(row_count * index_width);
	for s in container.iter_str() {
		let idx = dict_map[s];
		match index_width {
			1 => data.push(idx as u8),
			2 => data.extend_from_slice(&(idx as u16).to_le_bytes()),
			4 => data.extend_from_slice(&idx.to_le_bytes()),
			_ => unreachable!(),
		}
	}

	// Build dictionary table: dict_count + offsets + concatenated entries
	let mut extra = Vec::new();
	extra.extend_from_slice(&(dict_count as u32).to_le_bytes());

	// Write offset array for dictionary entries
	let mut offset: u32 = 0;
	extra.extend_from_slice(&offset.to_le_bytes());
	for entry in &dict_entries {
		offset += entry.len() as u32;
		extra.extend_from_slice(&offset.to_le_bytes());
	}

	// Write concatenated dictionary entry bytes
	for entry in &dict_entries {
		extra.extend_from_slice(entry.as_bytes());
	}

	Some(DictEncoded {
		data,
		extra,
		type_code: Type::Utf8.to_u8(),
		flags_bits,
	})
}

/// Try to dictionary-encode a Blob column. Returns None if not beneficial.
pub fn try_dict_encode_blob(container: &BlobContainer, min_ratio: f64) -> Option<DictEncoded> {
	let row_count = container.len();
	if row_count == 0 {
		return None;
	}

	// Build dictionary using byte slices
	let mut dict_map: HashMap<&[u8], u32> = HashMap::new();
	let mut dict_entries: Vec<&[u8]> = Vec::new();

	for bytes in container.iter_bytes() {
		if !dict_map.contains_key(bytes) {
			let idx = dict_entries.len() as u32;
			dict_map.insert(bytes, idx);
			dict_entries.push(bytes);
		}
	}

	let dict_count = dict_entries.len();

	if dict_count as f64 >= row_count as f64 * min_ratio {
		return None;
	}

	let (index_width, flags_bits) = if dict_count <= 255 {
		(1usize, dict_index_width_to_flags(1))
	} else if dict_count <= 65535 {
		(2, dict_index_width_to_flags(2))
	} else {
		(4, dict_index_width_to_flags(4))
	};

	let mut data = Vec::with_capacity(row_count * index_width);
	for bytes in container.iter_bytes() {
		let idx = dict_map[bytes];
		match index_width {
			1 => data.push(idx as u8),
			2 => data.extend_from_slice(&(idx as u16).to_le_bytes()),
			4 => data.extend_from_slice(&idx.to_le_bytes()),
			_ => unreachable!(),
		}
	}

	let mut extra = Vec::new();
	extra.extend_from_slice(&(dict_count as u32).to_le_bytes());

	let mut offset: u32 = 0;
	extra.extend_from_slice(&offset.to_le_bytes());
	for entry in &dict_entries {
		offset += entry.len() as u32;
		extra.extend_from_slice(&offset.to_le_bytes());
	}
	for entry in &dict_entries {
		extra.extend_from_slice(entry);
	}

	Some(DictEncoded {
		data,
		extra,
		type_code: Type::Blob.to_u8(),
		flags_bits,
	})
}

/// Try to dictionary-encode a sequence of byte vectors. Returns None if not beneficial.
pub fn try_dict_encode_bytes(serialized: &[Vec<u8>], type_code: u8, min_ratio: f64) -> Option<DictEncoded> {
	if serialized.is_empty() {
		return None;
	}

	let mut dict_map: HashMap<&[u8], u32> = HashMap::new();
	let mut dict_entries: Vec<&[u8]> = Vec::new();

	for bytes in serialized {
		if !dict_map.contains_key(bytes.as_slice()) {
			let idx = dict_entries.len() as u32;
			dict_map.insert(bytes.as_slice(), idx);
			dict_entries.push(bytes.as_slice());
		}
	}

	let dict_count = dict_entries.len();

	if dict_count as f64 >= serialized.len() as f64 * min_ratio {
		return None;
	}

	let (index_width, flags_bits) = if dict_count <= 255 {
		(1usize, dict_index_width_to_flags(1))
	} else if dict_count <= 65535 {
		(2, dict_index_width_to_flags(2))
	} else {
		(4, dict_index_width_to_flags(4))
	};

	let mut data = Vec::with_capacity(serialized.len() * index_width);
	for bytes in serialized {
		let idx = dict_map[bytes.as_slice()];
		match index_width {
			1 => data.push(idx as u8),
			2 => data.extend_from_slice(&(idx as u16).to_le_bytes()),
			4 => data.extend_from_slice(&idx.to_le_bytes()),
			_ => unreachable!(),
		}
	}

	let mut extra = Vec::new();
	extra.extend_from_slice(&(dict_count as u32).to_le_bytes());
	let mut offset: u32 = 0;
	extra.extend_from_slice(&offset.to_le_bytes());
	for entry in &dict_entries {
		offset += entry.len() as u32;
		extra.extend_from_slice(&offset.to_le_bytes());
	}
	for entry in &dict_entries {
		extra.extend_from_slice(entry);
	}

	Some(DictEncoded {
		data,
		extra,
		type_code,
		flags_bits,
	})
}

/// Convert DictEncoded into a PlainEncoded (used by the encoder pipeline).
impl DictEncoded {
	pub fn into_plain_encoded(self) -> PlainEncoded {
		PlainEncoded {
			data: self.data,
			offsets: vec![],
			nones: vec![],
			type_code: self.type_code,
			has_nones: false,
		}
	}
}

/// Decode a dictionary-encoded column back to strings.
///
/// - `data`: index array bytes
/// - `extra`: dictionary table bytes
/// - `row_count`: number of rows
/// - `index_width`: 1, 2, or 4 (from column flags)
pub fn decode_dict_utf8(
	data: &[u8],
	extra: &[u8],
	row_count: usize,
	index_width: usize,
) -> Result<Vec<String>, DecodeError> {
	let dict_entries = decode_dict_table(extra)?;

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
		values.push(dict_entries[idx].clone());
	}

	Ok(values)
}

/// Decode a dictionary-encoded column back to blobs.
pub fn decode_dict_blob(
	data: &[u8],
	extra: &[u8],
	row_count: usize,
	index_width: usize,
) -> Result<Vec<Blob>, DecodeError> {
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
		values.push(Blob::new(dict_entries[idx].clone()));
	}

	Ok(values)
}

fn decode_dict_table(extra: &[u8]) -> Result<Vec<String>, DecodeError> {
	if extra.len() < 4 {
		return Err(DecodeError::InvalidData("dict table too short".to_string()));
	}

	let dict_count = u32::from_le_bytes([extra[0], extra[1], extra[2], extra[3]]) as usize;
	let offsets_start = 4;
	let offsets_end = offsets_start + (dict_count + 1) * 4;

	if extra.len() < offsets_end {
		return Err(DecodeError::InvalidData("dict offsets truncated".to_string()));
	}

	let mut offsets = Vec::with_capacity(dict_count + 1);
	for i in 0..=dict_count {
		let pos = offsets_start + i * 4;
		offsets.push(u32::from_le_bytes([extra[pos], extra[pos + 1], extra[pos + 2], extra[pos + 3]]) as usize);
	}

	let data_start = offsets_end;
	let mut entries = Vec::with_capacity(dict_count);
	for i in 0..dict_count {
		let start = data_start + offsets[i];
		let end = data_start + offsets[i + 1];
		let s = str::from_utf8(&extra[start..end])
			.map_err(|e| DecodeError::InvalidData(format!("invalid dict UTF-8: {}", e)))?;
		entries.push(s.to_string());
	}

	Ok(entries)
}

pub fn decode_dict_table_bytes(extra: &[u8]) -> Result<Vec<Vec<u8>>, DecodeError> {
	if extra.len() < 4 {
		return Err(DecodeError::InvalidData("dict table too short".to_string()));
	}

	let dict_count = u32::from_le_bytes([extra[0], extra[1], extra[2], extra[3]]) as usize;
	let offsets_start = 4;
	let offsets_end = offsets_start + (dict_count + 1) * 4;

	if extra.len() < offsets_end {
		return Err(DecodeError::InvalidData("dict offsets truncated".to_string()));
	}

	let mut offsets = Vec::with_capacity(dict_count + 1);
	for i in 0..=dict_count {
		let pos = offsets_start + i * 4;
		offsets.push(u32::from_le_bytes([extra[pos], extra[pos + 1], extra[pos + 2], extra[pos + 3]]) as usize);
	}

	let data_start = offsets_end;
	let mut entries = Vec::with_capacity(dict_count);
	for i in 0..dict_count {
		let start = data_start + offsets[i];
		let end = data_start + offsets[i + 1];
		entries.push(extra[start..end].to_vec());
	}

	Ok(entries)
}

pub fn read_index(data: &[u8], i: usize, width: usize) -> u32 {
	let off = i * width;
	match width {
		1 => data[off] as u32,
		2 => u16::from_le_bytes([data[off], data[off + 1]]) as u32,
		4 => u32::from_le_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]]),
		_ => unreachable!(),
	}
}
