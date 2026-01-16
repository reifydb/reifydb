// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod subscription {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::layout::EncodedValuesLayout;
	use reifydb_type::value::r#type::Type;

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
	use reifydb_core::encoded::layout::EncodedValuesLayout;
	use reifydb_type::value::r#type::Type;

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
