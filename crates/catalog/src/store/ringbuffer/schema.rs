// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use once_cell::sync::Lazy;
use reifydb_core::encoded::schema::{RowSchema, RowSchemaField};
use reifydb_type::value::r#type::Type;

pub(crate) mod ringbuffer {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAMESPACE: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const CAPACITY: usize = 3;
	pub(crate) const PRIMARY_KEY: usize = 4;
	pub(crate) const PARTITION_BY: usize = 5;

	pub(crate) static SCHEMA: Lazy<RowSchema> = Lazy::new(|| {
		RowSchema::new(vec![
			RowSchemaField::unconstrained("id", Type::Uint8),
			RowSchemaField::unconstrained("namespace", Type::Uint8),
			RowSchemaField::unconstrained("name", Type::Utf8),
			RowSchemaField::unconstrained("capacity", Type::Uint8),
			RowSchemaField::unconstrained("primary_key", Type::Uint8),
			RowSchemaField::unconstrained("partition_by", Type::Utf8),
		])
	});
}

pub(crate) mod ringbuffer_namespace {
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

pub(crate) mod ringbuffer_metadata {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const CAPACITY: usize = 1;
	pub(crate) const HEAD: usize = 2;
	pub(crate) const TAIL: usize = 3;
	pub(crate) const COUNT: usize = 4;

	pub(crate) static SCHEMA: Lazy<RowSchema> = Lazy::new(|| {
		RowSchema::new(vec![
			RowSchemaField::unconstrained("id", Type::Uint8),
			RowSchemaField::unconstrained("capacity", Type::Uint8),
			RowSchemaField::unconstrained("head", Type::Uint8),
			RowSchemaField::unconstrained("tail", Type::Uint8),
			RowSchemaField::unconstrained("count", Type::Uint8),
		])
	});
}
