// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// View columns have a simpler layout than view columns - no auto_increment
pub(crate) mod view_column {
	use once_cell::sync::Lazy;
	use reifydb_core::{Type, row::EncodedRowLayout};

	pub(crate) const ID: usize = 0;
	pub(crate) const VIEW: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const VALUE: usize = 3;
	pub(crate) const INDEX: usize = 4;

	pub(crate) static LAYOUT: Lazy<EncodedRowLayout> = Lazy::new(|| {
		EncodedRowLayout::new(&[
			Type::Uint8, // id
			Type::Uint8, // view
			Type::Utf8,  // name
			Type::Uint1, // value (type)
			Type::Uint2, // index
		])
	});
}

// Reuse ViewColumnKey infrastructure for view-column relationships
pub(crate) mod view_column_link {
	use once_cell::sync::Lazy;
	use reifydb_core::{Type, row::EncodedRowLayout};

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;
	pub(crate) const INDEX: usize = 2;

	pub(crate) static LAYOUT: Lazy<EncodedRowLayout> = Lazy::new(|| {
		EncodedRowLayout::new(&[
			Type::Uint8, // column id
			Type::Utf8,  // column name
			Type::Uint2, // column index - position in the view
		])
	});
}
