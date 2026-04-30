// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

/// Magic bytes: "RBCF" in ASCII (0x52, 0x42, 0x43, 0x46).
pub const RBCF_MAGIC: u32 = 0x46434252; // little-endian: reads as "RBCF"

/// Current format version.
pub const RBCF_VERSION: u16 = 1;

/// Size of the message header in bytes.
pub const MESSAGE_HEADER_SIZE: usize = 16;

/// Size of a frame header in bytes.
pub const FRAME_HEADER_SIZE: usize = 12;

/// Size of a column descriptor in bytes.
pub const COLUMN_DESCRIPTOR_SIZE: usize = 28;

/// Meta flag: frame has row_numbers array.
pub const META_HAS_ROW_NUMBERS: u8 = 1 << 0;

/// Meta flag: frame has created_at array.
pub const META_HAS_CREATED_AT: u8 = 1 << 1;

/// Meta flag: frame has updated_at array.
pub const META_HAS_UPDATED_AT: u8 = 1 << 2;

/// Column flag: column has nones (has nones bitmap for Option types).
pub const COL_FLAG_HAS_NONES: u8 = 1 << 0;

/// Per-column encoding method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Encoding {
	/// Raw little-endian values. Variable-length types use u32 offset array + data.
	/// Bool is bit-packed.
	Plain = 0,
	/// Dictionary encoding: dictionary table in `extra`, index array in `data`.
	Dict = 1,
	/// Run-length encoding: (value, u32 run_length) pairs. Fixed-size types only.
	Rle = 2,
	/// Delta encoding: 1-byte delta_width + baseline + signed LE deltas.
	Delta = 3,
	/// Bit-packing (reserved, same as Plain for Bool).
	BitPack = 4,
	/// Delta + RLE on the deltas.
	DeltaRle = 5,
}

impl Encoding {
	pub fn from_u8(v: u8) -> Option<Encoding> {
		match v {
			0 => Some(Encoding::Plain),
			1 => Some(Encoding::Dict),
			2 => Some(Encoding::Rle),
			3 => Some(Encoding::Delta),
			4 => Some(Encoding::BitPack),
			5 => Some(Encoding::DeltaRle),
			_ => None,
		}
	}
}

/// Extract the dictionary index width from column flags.
/// Bits 4-5: 0=u8, 1=u16, 2=u32.
pub fn dict_index_width_from_flags(flags: u8) -> usize {
	match (flags >> 4) & 0x03 {
		0 => 1, // u8
		1 => 2, // u16
		2 => 4, // u32
		_ => 4, // fallback to u32
	}
}

/// Encode the dictionary index width into flags bits 4-5.
pub fn dict_index_width_to_flags(width: usize) -> u8 {
	match width {
		1 => 0 << 4,
		2 => 1 << 4,
		4 => 2 << 4,
		_ => 2 << 4,
	}
}
