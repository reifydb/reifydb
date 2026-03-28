// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod identity {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{RowSchema, RowSchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const IDENTITY: usize = 0;
	pub(crate) const NAME: usize = 1;
	pub(crate) const ENABLED: usize = 2;

	pub(crate) static SCHEMA: Lazy<RowSchema> = Lazy::new(|| {
		RowSchema::new(vec![
			RowSchemaField::unconstrained("identity", Type::IdentityId),
			RowSchemaField::unconstrained("name", Type::Utf8),
			RowSchemaField::unconstrained("enabled", Type::Boolean),
		])
	});
}
