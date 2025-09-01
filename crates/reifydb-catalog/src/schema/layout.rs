// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub(crate) mod schema {
	use once_cell::sync::Lazy;
	use reifydb_core::row::EncodedRowLayout;
	use reifydb_type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;

	pub(crate) static LAYOUT: Lazy<EncodedRowLayout> = Lazy::new(|| {
		EncodedRowLayout::new(&[
			Type::Uint8, // id
			Type::Utf8,  // name
		])
	});
}
