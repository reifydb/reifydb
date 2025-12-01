// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use once_cell::sync::Lazy;
use reifydb_core::value::encoded::EncodedValuesLayout;
use reifydb_type::Type;

pub(crate) mod dictionary {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAMESPACE: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const VALUE_TYPE: usize = 3;
	pub(crate) const ID_TYPE: usize = 4;

	pub(crate) static LAYOUT: Lazy<EncodedValuesLayout> = Lazy::new(|| {
		EncodedValuesLayout::new(&[
			Type::Uint8, // id
			Type::Uint8, // namespace id
			Type::Utf8,  // name
			Type::Uint1, // value_type (Type enum ordinal)
			Type::Uint1, // id_type (Type enum ordinal)
		])
	});
}

pub(crate) mod dictionary_namespace {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;

	pub(crate) static LAYOUT: Lazy<EncodedValuesLayout> = Lazy::new(|| {
		EncodedValuesLayout::new(&[
			Type::Uint8, // id
			Type::Utf8,  // name
		])
	});
}
