// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub(crate) mod column {
	use once_cell::sync::Lazy;
	use reifydb_core::value::encoded::EncodedValuesLayout;
	use reifydb_type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const TABLE: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const VALUE: usize = 3;
	pub(crate) const INDEX: usize = 4;
	pub(crate) const AUTO_INCREMENT: usize = 5;
	pub(crate) const CONSTRAINT: usize = 6;

	pub(crate) static LAYOUT: Lazy<EncodedValuesLayout> = Lazy::new(|| {
		EncodedValuesLayout::new(&[
			Type::Uint8,   // id
			Type::Uint8,   // table
			Type::Utf8,    // name
			Type::Uint1,   // value (type enum)
			Type::Uint2,   // index
			Type::Boolean, // auto_increment
			Type::Blob,    // constraint
		])
	});
}

pub(crate) mod table_column {
	use once_cell::sync::Lazy;
	use reifydb_core::value::encoded::EncodedValuesLayout;
	use reifydb_type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;
	pub(crate) const INDEX: usize = 2;

	pub(crate) static LAYOUT: Lazy<EncodedValuesLayout> = Lazy::new(|| {
		EncodedValuesLayout::new(&[
			Type::Uint8, // column id
			Type::Utf8,  // column name
			Type::Uint2, // column index - position in the table
		])
	});
}
