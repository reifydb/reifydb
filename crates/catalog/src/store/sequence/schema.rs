// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub(crate) mod sequence {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{Schema, SchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const VALUE: usize = 0;

	pub(crate) static SCHEMA: Lazy<Schema> =
		Lazy::new(|| Schema::new(vec![SchemaField::unconstrained("value", Type::Uint8)]));
}
