// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod authentication {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{RowSchema, RowSchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const IDENTITY: usize = 1;
	pub(crate) const METHOD: usize = 2;
	pub(crate) const PROPERTIES: usize = 3;

	pub(crate) static SCHEMA: Lazy<RowSchema> = Lazy::new(|| {
		RowSchema::new(vec![
			RowSchemaField::unconstrained("id", Type::Uint8),
			RowSchemaField::unconstrained("identity", Type::IdentityId),
			RowSchemaField::unconstrained("method", Type::Utf8),
			RowSchemaField::unconstrained("properties", Type::Utf8),
		])
	});
}
