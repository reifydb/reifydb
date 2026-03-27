// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use once_cell::sync::Lazy;
use reifydb_core::encoded::schema::{RowSchema, RowSchemaField};
use reifydb_type::value::r#type::Type;

pub(crate) mod migration {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;
	pub(crate) const BODY: usize = 2;
	pub(crate) const ROLLBACK_BODY: usize = 3;

	pub(crate) static SCHEMA: Lazy<RowSchema> = Lazy::new(|| {
		RowSchema::new(vec![
			RowSchemaField::unconstrained("id", Type::Uint8),
			RowSchemaField::unconstrained("name", Type::Utf8),
			RowSchemaField::unconstrained("body", Type::Utf8),
			RowSchemaField::unconstrained("rollback_body", Type::Utf8),
		])
	});
}

pub(crate) mod migration_event {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const MIGRATION_ID: usize = 1;
	pub(crate) const ACTION: usize = 2;

	pub(crate) static SCHEMA: Lazy<RowSchema> = Lazy::new(|| {
		RowSchema::new(vec![
			RowSchemaField::unconstrained("id", Type::Uint8),
			RowSchemaField::unconstrained("migration_id", Type::Uint8),
			RowSchemaField::unconstrained("action", Type::Uint1),
		])
	});
}
