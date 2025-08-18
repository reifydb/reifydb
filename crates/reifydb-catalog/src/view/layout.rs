// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub(crate) mod view {
	use once_cell::sync::Lazy;
	use reifydb_core::{Type, row::EncodedRowLayout};

	pub(crate) const ID: usize = 0;
	pub(crate) const SCHEMA: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const KIND: usize = 3;

	pub(crate) static LAYOUT: Lazy<EncodedRowLayout> = Lazy::new(|| {
		EncodedRowLayout::new(&[
			Type::Uint8, // id
			Type::Uint8, // schema id
			Type::Utf8,  // name
			Type::Uint1, // kind (0 = Deferred, 1 = Transactional)
		])
	});
}

pub(crate) mod view_schema {
	use once_cell::sync::Lazy;
	use reifydb_core::{Type, row::EncodedRowLayout};

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;

	pub(crate) static LAYOUT: Lazy<EncodedRowLayout> = Lazy::new(|| {
		EncodedRowLayout::new(&[
			Type::Uint8, // id
			Type::Utf8,  // name
		])
	});
}
