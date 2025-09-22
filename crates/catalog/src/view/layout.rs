// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub(crate) mod view {
	use once_cell::sync::Lazy;
	use reifydb_core::value::row::EncodedRowLayout;
	use reifydb_type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAMESPACE: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const KIND: usize = 3;
	pub(crate) const PRIMARY_KEY: usize = 4;

	pub(crate) static LAYOUT: Lazy<EncodedRowLayout> = Lazy::new(|| {
		EncodedRowLayout::new(&[
			Type::Uint8, // id
			Type::Uint8, // namespace id
			Type::Utf8,  // name
			Type::Uint1, // kind (0 = Deferred, 1 = Transactional)
			Type::Uint8, // primary_key
		])
	});
}

pub(crate) mod view_namespace {
	use once_cell::sync::Lazy;
	use reifydb_core::value::row::EncodedRowLayout;
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
