// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

//! WASM-safe flat binary format types for columnar data
//!
//! These types mirror ColumnsFFI/ColumnFFI/ColumnDataFFI but use u32 offsets
//! instead of host pointers, making them suitable for WASM's 32-bit linear memory.
//! All values are little-endian.

/// Size of the ColumnsWasm header in bytes.
pub const COLUMNS_WASM_HEADER_SIZE: usize = 16;

/// Size of each ColumnWasm entry in bytes.
pub const COLUMN_WASM_SIZE: usize = 40;

/// Flat binary header for a columnar batch, using u32 offsets instead of pointers.
///
/// Layout (16 bytes, all little-endian u32):
/// ```text
/// [0..4]   row_count
/// [4..8]   column_count
/// [8..12]  row_numbers_offset  (from buffer start, 0 = absent)
/// [12..16] row_numbers_len     (bytes)
/// ```
pub struct ColumnsWasm {
	pub row_count: u32,
	pub column_count: u32,
	pub row_numbers_offset: u32,
	pub row_numbers_len: u32,
}

impl ColumnsWasm {
	pub fn read_from_bytes(bytes: &[u8]) -> Self {
		assert!(bytes.len() >= COLUMNS_WASM_HEADER_SIZE);
		Self {
			row_count: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
			column_count: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
			row_numbers_offset: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
			row_numbers_len: u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
		}
	}

	pub fn write_to_bytes(&self, buf: &mut Vec<u8>) {
		buf.extend_from_slice(&self.row_count.to_le_bytes());
		buf.extend_from_slice(&self.column_count.to_le_bytes());
		buf.extend_from_slice(&self.row_numbers_offset.to_le_bytes());
		buf.extend_from_slice(&self.row_numbers_len.to_le_bytes());
	}
}

/// Flat binary column descriptor, using u32 offsets instead of pointers.
///
/// Layout (40 bytes, all little-endian u32):
/// ```text
/// [0..4]   name_offset
/// [4..8]   name_len
/// [8..12]  type_code  (ColumnTypeCode discriminant)
/// [12..16] data_row_count
/// [16..20] data_offset
/// [20..24] data_len
/// [24..28] bitvec_offset
/// [28..32] bitvec_len
/// [32..36] offsets_offset
/// [36..40] offsets_len
/// ```
pub struct ColumnWasm {
	pub name_offset: u32,
	pub name_len: u32,
	pub type_code: u32,
	pub data_row_count: u32,
	pub data_offset: u32,
	pub data_len: u32,
	pub bitvec_offset: u32,
	pub bitvec_len: u32,
	pub offsets_offset: u32,
	pub offsets_len: u32,
}

impl ColumnWasm {
	pub fn read_from_bytes(bytes: &[u8]) -> Self {
		assert!(bytes.len() >= COLUMN_WASM_SIZE);
		Self {
			name_offset: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
			name_len: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
			type_code: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
			data_row_count: u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
			data_offset: u32::from_le_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]),
			data_len: u32::from_le_bytes([bytes[20], bytes[21], bytes[22], bytes[23]]),
			bitvec_offset: u32::from_le_bytes([bytes[24], bytes[25], bytes[26], bytes[27]]),
			bitvec_len: u32::from_le_bytes([bytes[28], bytes[29], bytes[30], bytes[31]]),
			offsets_offset: u32::from_le_bytes([bytes[32], bytes[33], bytes[34], bytes[35]]),
			offsets_len: u32::from_le_bytes([bytes[36], bytes[37], bytes[38], bytes[39]]),
		}
	}

	pub fn write_to_bytes(&self, buf: &mut Vec<u8>) {
		buf.extend_from_slice(&self.name_offset.to_le_bytes());
		buf.extend_from_slice(&self.name_len.to_le_bytes());
		buf.extend_from_slice(&self.type_code.to_le_bytes());
		buf.extend_from_slice(&self.data_row_count.to_le_bytes());
		buf.extend_from_slice(&self.data_offset.to_le_bytes());
		buf.extend_from_slice(&self.data_len.to_le_bytes());
		buf.extend_from_slice(&self.bitvec_offset.to_le_bytes());
		buf.extend_from_slice(&self.bitvec_len.to_le_bytes());
		buf.extend_from_slice(&self.offsets_offset.to_le_bytes());
		buf.extend_from_slice(&self.offsets_len.to_le_bytes());
	}

	/// Write this column descriptor at a specific position in the buffer.
	pub fn write_at(&self, buf: &mut [u8], offset: usize) {
		let b = &mut buf[offset..offset + COLUMN_WASM_SIZE];
		b[0..4].copy_from_slice(&self.name_offset.to_le_bytes());
		b[4..8].copy_from_slice(&self.name_len.to_le_bytes());
		b[8..12].copy_from_slice(&self.type_code.to_le_bytes());
		b[12..16].copy_from_slice(&self.data_row_count.to_le_bytes());
		b[16..20].copy_from_slice(&self.data_offset.to_le_bytes());
		b[20..24].copy_from_slice(&self.data_len.to_le_bytes());
		b[24..28].copy_from_slice(&self.bitvec_offset.to_le_bytes());
		b[28..32].copy_from_slice(&self.bitvec_len.to_le_bytes());
		b[32..36].copy_from_slice(&self.offsets_offset.to_le_bytes());
		b[36..40].copy_from_slice(&self.offsets_len.to_le_bytes());
	}
}
