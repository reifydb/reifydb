// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Deref;

use reifydb_value::util::hash::{Hash64, xxh3_64};
use serde::{Deserialize, Serialize};

use crate::{
	constraint::type_constraint_to_extern_c,
	row::shape::{RowFamily, RowShapeField},
};

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RowShapeFingerprint(pub Hash64);

impl Deref for RowShapeFingerprint {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0.0
	}
}

impl RowShapeFingerprint {
	#[inline]
	pub const fn new(value: u64) -> Self {
		Self(Hash64(value))
	}

	#[inline]
	pub const fn zero() -> Self {
		Self(Hash64(0))
	}

	#[inline]
	pub const fn as_u64(&self) -> u64 {
		self.0.0
	}

	#[inline]
	pub const fn to_le_bytes(&self) -> [u8; 8] {
		self.0.0.to_le_bytes()
	}

	#[inline]
	pub const fn from_le_bytes(bytes: [u8; 8]) -> Self {
		Self(Hash64(u64::from_le_bytes(bytes)))
	}
}

impl From<Hash64> for RowShapeFingerprint {
	fn from(hash: Hash64) -> Self {
		Self(hash)
	}
}

impl From<RowShapeFingerprint> for Hash64 {
	fn from(fp: RowShapeFingerprint) -> Self {
		fp.0
	}
}

impl From<u64> for RowShapeFingerprint {
	fn from(value: u64) -> Self {
		Self(Hash64(value))
	}
}

pub fn compute_fingerprint(family: RowFamily, fields: &[RowShapeField]) -> RowShapeFingerprint {
	let estimated_size = 3 + fields.len() * 42;
	let mut buffer = Vec::with_capacity(estimated_size);

	buffer.push(family as u8);

	let field_count = fields.len() as u16;
	buffer.extend_from_slice(&field_count.to_le_bytes());

	for field in fields {
		let name_bytes = field.name.as_bytes();
		let name_len = name_bytes.len() as u16;
		buffer.extend_from_slice(&name_len.to_le_bytes());
		buffer.extend_from_slice(name_bytes);

		let extern_c = type_constraint_to_extern_c(&field.constraint)
			.expect("row shape field constraint exceeds tag capacity");
		buffer.push(extern_c.base_type);
		buffer.push(extern_c.constraint_type);
		buffer.extend_from_slice(&extern_c.constraint_param1.to_le_bytes());
		buffer.extend_from_slice(&extern_c.constraint_param2.to_le_bytes());
	}

	RowShapeFingerprint(xxh3_64(&buffer))
}
