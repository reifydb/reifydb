// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub(crate) mod flow {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::layout::EncodedValuesLayout;
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAMESPACE: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const STATUS: usize = 3;

	pub(crate) static LAYOUT: Lazy<EncodedValuesLayout> = Lazy::new(|| {
		EncodedValuesLayout::new(&[
			Type::Uint8, // id
			Type::Uint8, // namespace id
			Type::Utf8,  // name
			Type::Uint1, // status (0 = Active, 1 = Paused, 2 = Failed)
		])
	});
}

pub(crate) mod flow_namespace {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::layout::EncodedValuesLayout;
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;

	pub(crate) static LAYOUT: Lazy<EncodedValuesLayout> = Lazy::new(|| {
		EncodedValuesLayout::new(&[
			Type::Uint8, // id
			Type::Utf8,  // name
		])
	});
}
