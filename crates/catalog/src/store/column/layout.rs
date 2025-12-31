// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub(crate) mod column {
	use once_cell::sync::Lazy;
	use reifydb_core::value::encoded::EncodedValuesLayout;
	use reifydb_type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const SOURCE: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const VALUE: usize = 3;
	pub(crate) const INDEX: usize = 4;
	pub(crate) const AUTO_INCREMENT: usize = 5;
	pub(crate) const CONSTRAINT: usize = 6;
	pub(crate) const DICTIONARY_ID: usize = 7; // 0 = no dictionary, else dictionary_id

	pub(crate) static LAYOUT: Lazy<EncodedValuesLayout> = Lazy::new(|| {
		EncodedValuesLayout::new(&[
			Type::Uint8,   // id
			Type::Uint8,   // source
			Type::Utf8,    // name
			Type::Uint1,   // value (type enum)
			Type::Uint1,   // index
			Type::Boolean, // auto_increment
			Type::Blob,    // constraint
			Type::Uint8,   // dictionary_id (0 = no dictionary)
		])
	});
}

pub(crate) mod source_column {
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
			Type::Uint1, // column index - position in the table
		])
	});
}
