// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Storage layouts for schema data.

pub(crate) mod schema_header {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::layout::EncodedValuesLayout;
	use reifydb_type::value::r#type::Type;

	/// Field count index
	pub(crate) const FIELD_COUNT: usize = 0;

	pub(crate) static LAYOUT: Lazy<EncodedValuesLayout> = Lazy::new(|| {
		EncodedValuesLayout::new(&[
			Type::Uint2, // field_count (u16)
		])
	});
}

pub(crate) mod schema_field {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::layout::EncodedValuesLayout;
	use reifydb_type::value::r#type::Type;

	/// Field name index
	pub(crate) const NAME: usize = 0;
	/// Field type index
	pub(crate) const FIELD_TYPE: usize = 1;
	/// Field offset index
	pub(crate) const OFFSET: usize = 2;
	/// Field size index
	pub(crate) const SIZE: usize = 3;
	/// Field alignment index
	pub(crate) const ALIGN: usize = 4;

	pub(crate) static LAYOUT: Lazy<EncodedValuesLayout> = Lazy::new(|| {
		EncodedValuesLayout::new(&[
			Type::Utf8,  // name
			Type::Uint1, // field_type (Type::to_u8())
			Type::Uint4, // offset
			Type::Uint4, // size
			Type::Uint1, // align
		])
	});
}
