// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use once_cell::sync::Lazy;
use reifydb_core::encoded::schema::{RowSchema, RowSchemaField};
use reifydb_type::value::r#type::Type;

pub(crate) mod dictionary {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAMESPACE: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const VALUE_TYPE: usize = 3;
	pub(crate) const ID_TYPE: usize = 4;

	pub(crate) static SCHEMA: Lazy<RowSchema> = Lazy::new(|| {
		RowSchema::new(vec![
			RowSchemaField::unconstrained("id", Type::Uint8),
			RowSchemaField::unconstrained("namespace", Type::Uint8),
			RowSchemaField::unconstrained("name", Type::Utf8),
			RowSchemaField::unconstrained("value_type", Type::Uint1),
			RowSchemaField::unconstrained("id_type", Type::Uint1),
		])
	});
}

pub(crate) mod dictionary_namespace {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;

	pub(crate) static SCHEMA: Lazy<RowSchema> = Lazy::new(|| {
		RowSchema::new(vec![
			RowSchemaField::unconstrained("id", Type::Uint8),
			RowSchemaField::unconstrained("name", Type::Utf8),
		])
	});
}
