// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub(crate) mod sequence {
	use once_cell::sync::Lazy;
	use reifydb_core::value::row::EncodedRowLayout;
	use reifydb_type::Type;

	pub(crate) const VALUE: usize = 0;

	pub(crate) static LAYOUT: Lazy<EncodedRowLayout> = Lazy::new(|| {
		EncodedRowLayout::new(&[
			Type::Uint8, // VALUE
		])
	});
}
