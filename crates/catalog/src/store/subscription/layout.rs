// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod subscription {
	use once_cell::sync::Lazy;
	use reifydb_core::value::encoded::EncodedValuesLayout;
	use reifydb_type::Type;

	pub const ID: usize = 0;
	pub const ACKNOWLEDGED_VERSION: usize = 1;
	pub const PRIMARY_KEY: usize = 2;

	pub static LAYOUT: Lazy<EncodedValuesLayout> = Lazy::new(|| {
		EncodedValuesLayout::new(&[
			Type::Uuid7, // id - UUID v7 (16 bytes)
			Type::Uint8, // acknowledged_version
			Type::Uint8, // primary_key
		])
	});
}

pub mod subscription_column {
	use once_cell::sync::Lazy;
	use reifydb_core::value::encoded::EncodedValuesLayout;
	use reifydb_type::Type;

	pub const ID: usize = 0;
	pub const NAME: usize = 1;
	pub const TYPE: usize = 2;

	pub static LAYOUT: Lazy<EncodedValuesLayout> = Lazy::new(|| {
		EncodedValuesLayout::new(&[
			Type::Uint8, // id
			Type::Utf8,  // name
			Type::Uint1, // type (stored as u8)
		])
	});
}

/// Layout for subscription delta entries
/// Stores Insert/Update/Delete operations with pre/post values
pub mod subscription_delta {
	use once_cell::sync::Lazy;
	use reifydb_core::value::encoded::EncodedValuesLayout;
	use reifydb_type::Type;

	/// Operation type: 0=Insert, 1=Update, 2=Delete
	pub const OP: usize = 0;
	/// Pre-image values (for Update/Delete), null for Insert
	pub const PRE: usize = 1;
	/// Post-image values (for Insert/Update), null for Delete
	pub const POST: usize = 2;

	pub static LAYOUT: Lazy<EncodedValuesLayout> = Lazy::new(|| {
		EncodedValuesLayout::new(&[
			Type::Uint1, // op: 0=Insert, 1=Update, 2=Delete
			Type::Blob,  // pre: encoded pre-values (null for Insert)
			Type::Blob,  // post: encoded post-values (null for Delete)
		])
	});

	/// Operation type constants
	pub const OP_INSERT: u8 = 0;
	pub const OP_UPDATE: u8 = 1;
	pub const OP_DELETE: u8 = 2;
}
