// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

/// Magic bytes: "RBCF" in ASCII (0x52, 0x42, 0x43, 0x46).
pub const RBCF_MAGIC: u32 = 0x46434252;

pub const RBCF_VERSION: u16 = 1;

pub const MESSAGE_HEADER_SIZE: usize = 16;

pub const FRAME_HEADER_SIZE: usize = 12;

pub const COLUMN_DESCRIPTOR_SIZE: usize = 28;

pub const META_HAS_ROW_NUMBERS: u8 = 1 << 0;

pub const META_HAS_CREATED_AT: u8 = 1 << 1;

pub const META_HAS_UPDATED_AT: u8 = 1 << 2;

pub const COL_FLAG_HAS_NONES: u8 = 1 << 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Encoding {
	Plain = 0,

	Dict = 1,

	Rle = 2,

	Delta = 3,

	BitPack = 4,

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

pub fn dict_index_width_from_flags(flags: u8) -> usize {
	match (flags >> 4) & 0x03 {
		0 => 1,
		1 => 2,
		2 => 4,
		_ => 4,
	}
}

pub fn dict_index_width_to_flags(width: usize) -> u8 {
	match width {
		1 => 0 << 4,
		2 => 1 << 4,
		4 => 2 << 4,
		_ => 2 << 4,
	}
}
