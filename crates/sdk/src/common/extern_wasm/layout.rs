// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub const EXTERN_WASM_COLUMNS_HEADER_SIZE: usize = 16;

pub const EXTERN_WASM_COLUMN_SIZE: usize = 37;

pub struct ExternWasmColumns {
	pub row_count: u32,
	pub column_count: u32,
	pub row_numbers_offset: u32,
	pub row_numbers_len: u32,
}

impl ExternWasmColumns {
	pub fn read_from_bytes(bytes: &[u8]) -> Self {
		assert!(bytes.len() >= EXTERN_WASM_COLUMNS_HEADER_SIZE);
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

pub struct ExternWasmColumn {
	pub name_offset: u32,
	pub name_len: u32,
	pub type_code: u8,
	pub data_row_count: u32,
	pub data_offset: u32,
	pub data_len: u32,
	pub bitvec_offset: u32,
	pub bitvec_len: u32,
	pub offsets_offset: u32,
	pub offsets_len: u32,
}

impl ExternWasmColumn {
	pub fn read_from_bytes(bytes: &[u8]) -> Self {
		assert!(bytes.len() >= EXTERN_WASM_COLUMN_SIZE);
		Self {
			name_offset: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
			name_len: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
			type_code: bytes[8],
			data_row_count: u32::from_le_bytes([bytes[9], bytes[10], bytes[11], bytes[12]]),
			data_offset: u32::from_le_bytes([bytes[13], bytes[14], bytes[15], bytes[16]]),
			data_len: u32::from_le_bytes([bytes[17], bytes[18], bytes[19], bytes[20]]),
			bitvec_offset: u32::from_le_bytes([bytes[21], bytes[22], bytes[23], bytes[24]]),
			bitvec_len: u32::from_le_bytes([bytes[25], bytes[26], bytes[27], bytes[28]]),
			offsets_offset: u32::from_le_bytes([bytes[29], bytes[30], bytes[31], bytes[32]]),
			offsets_len: u32::from_le_bytes([bytes[33], bytes[34], bytes[35], bytes[36]]),
		}
	}

	pub fn write_to_bytes(&self, buf: &mut Vec<u8>) {
		buf.extend_from_slice(&self.name_offset.to_le_bytes());
		buf.extend_from_slice(&self.name_len.to_le_bytes());
		buf.push(self.type_code);
		buf.extend_from_slice(&self.data_row_count.to_le_bytes());
		buf.extend_from_slice(&self.data_offset.to_le_bytes());
		buf.extend_from_slice(&self.data_len.to_le_bytes());
		buf.extend_from_slice(&self.bitvec_offset.to_le_bytes());
		buf.extend_from_slice(&self.bitvec_len.to_le_bytes());
		buf.extend_from_slice(&self.offsets_offset.to_le_bytes());
		buf.extend_from_slice(&self.offsets_len.to_le_bytes());
	}

	pub fn write_at(&self, buf: &mut [u8], offset: usize) {
		let b = &mut buf[offset..offset + EXTERN_WASM_COLUMN_SIZE];
		b[0..4].copy_from_slice(&self.name_offset.to_le_bytes());
		b[4..8].copy_from_slice(&self.name_len.to_le_bytes());
		b[8] = self.type_code;
		b[9..13].copy_from_slice(&self.data_row_count.to_le_bytes());
		b[13..17].copy_from_slice(&self.data_offset.to_le_bytes());
		b[17..21].copy_from_slice(&self.data_len.to_le_bytes());
		b[21..25].copy_from_slice(&self.bitvec_offset.to_le_bytes());
		b[25..29].copy_from_slice(&self.bitvec_len.to_le_bytes());
		b[29..33].copy_from_slice(&self.offsets_offset.to_le_bytes());
		b[33..37].copy_from_slice(&self.offsets_len.to_le_bytes());
	}
}
