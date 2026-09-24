// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crc32fast::Hasher;
use reifydb_value::{byte_size::ByteSize, reifydb_assertions, value::datetime::DateTime};

use crate::log::{LogIndex, LogVersion, Position};

pub const HEADER_BYTES: usize = 40;

pub const ENTRY_BYTES: usize = 12;

pub const MAGIC: u32 = u32::from_le_bytes(*b"RIDX");

pub const DEFAULT_INTERVAL: ByteSize = ByteSize::from_kib(4);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimestampRange {
	pub min: DateTime,
	pub max: DateTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
	pub magic: u32,
	pub base_version: LogVersion,
	pub base_index: LogIndex,
	pub timestamps: Option<TimestampRange>,
}

impl Header {
	pub fn new(base_version: LogVersion, base_index: LogIndex) -> Self {
		Self {
			magic: MAGIC,
			base_version,
			base_index,
			timestamps: None,
		}
	}

	pub fn decode(buf: &[u8; HEADER_BYTES]) -> Option<Self> {
		let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
		if magic != MAGIC {
			return None;
		}
		let found = u32::from_le_bytes(buf[4..8].try_into().unwrap());
		if found != checksum_bytes(&buf[8..]) {
			return None;
		}
		let min = DateTime::from_order(u64::from_le_bytes(buf[24..32].try_into().unwrap()));
		let max = DateTime::from_order(u64::from_le_bytes(buf[32..40].try_into().unwrap()));
		Some(Self {
			magic,
			base_version: LogVersion::new(u64::from_le_bytes(buf[8..16].try_into().unwrap())),
			base_index: LogIndex::new(u64::from_le_bytes(buf[16..24].try_into().unwrap())),
			timestamps: (min <= max).then_some(TimestampRange {
				min,
				max,
			}),
		})
	}

	pub fn encode(&self) -> [u8; HEADER_BYTES] {
		let (min, max) = match self.timestamps {
			Some(range) => (range.min, range.max),
			None => (DateTime::MAX, DateTime::MIN),
		};
		let mut out = [0u8; HEADER_BYTES];
		out[0..4].copy_from_slice(&self.magic.to_le_bytes());
		out[8..16].copy_from_slice(&self.base_version.as_u64().to_le_bytes());
		out[16..24].copy_from_slice(&self.base_index.as_u64().to_le_bytes());
		out[24..32].copy_from_slice(&min.to_order().to_le_bytes());
		out[32..40].copy_from_slice(&max.to_order().to_le_bytes());
		let checksum = checksum_bytes(&out[8..]);
		out[4..8].copy_from_slice(&checksum.to_le_bytes());
		out
	}
}

fn checksum_bytes(bytes: &[u8]) -> u32 {
	let mut hasher = Hasher::new();
	hasher.update(bytes);
	hasher.finalize()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry {
	pub version: LogVersion,
	pub index: LogIndex,
	pub position: Position,
}

pub fn encode_entry(header: &Header, entry: Entry) -> [u8; ENTRY_BYTES] {
	reifydb_assertions! {
		assert!(
			entry.version >= header.base_version,
			"version {} is below the base version {} the index was created with, and the delta wraps \
			 silently into an entry no lookup can follow",
			entry.version,
			header.base_version
		);
		assert!(
			entry.version.as_u64() - header.base_version.as_u64() <= u32::MAX as u64,
			"a delta of {} overflows the four byte delta field (limit={})",
			entry.version.as_u64() - header.base_version.as_u64(),
			u32::MAX
		);
		assert!(
			entry.index >= header.base_index,
			"index {} is below the base index {} the index was created with, and the delta wraps \
			 silently into an entry no lookup can follow",
			entry.index,
			header.base_index
		);
		assert!(
			entry.index.as_u64() - header.base_index.as_u64() <= u32::MAX as u64,
			"an index delta of {} overflows the four byte delta field (limit={})",
			entry.index.as_u64() - header.base_index.as_u64(),
			u32::MAX
		);
		assert!(
			entry.position.as_u64() <= u32::MAX as u64,
			"a position of {} overflows the four byte position field, so the segment must stay under \
			 {} bytes",
			entry.position,
			u32::MAX
		);
	}
	let mut out = [0u8; ENTRY_BYTES];
	out[0..4].copy_from_slice(&((entry.version.as_u64() - header.base_version.as_u64()) as u32).to_le_bytes());
	out[4..8].copy_from_slice(&((entry.index.as_u64() - header.base_index.as_u64()) as u32).to_le_bytes());
	out[8..12].copy_from_slice(&(entry.position.as_u64() as u32).to_le_bytes());
	out
}

pub fn decode_entry(header: &Header, buf: &[u8; ENTRY_BYTES]) -> Entry {
	Entry {
		version: LogVersion::new(
			header.base_version.as_u64() + u32::from_le_bytes(buf[0..4].try_into().unwrap()) as u64,
		),
		index: LogIndex::new(
			header.base_index.as_u64() + u32::from_le_bytes(buf[4..8].try_into().unwrap()) as u64,
		),
		position: Position::new(u32::from_le_bytes(buf[8..12].try_into().unwrap()) as u64),
	}
}
