// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub(crate) mod column_policy {
	use once_cell::sync::Lazy;
	use reifydb_core::value::encoded::EncodedValuesLayout;
	use reifydb_type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const COLUMN: usize = 1;
	pub(crate) const POLICY: usize = 2;
	pub(crate) const VALUE: usize = 3;

	pub(crate) static LAYOUT: Lazy<EncodedValuesLayout> = Lazy::new(|| {
		EncodedValuesLayout::new(&[
			Type::Uint8, // id
			Type::Uint8, // column
			Type::Uint1, // policy
			Type::Uint1, // value
		])
	});
}
