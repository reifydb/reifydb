// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use once_cell::sync::Lazy;
use reifydb_core::encoded::schema::{Schema, SchemaField};
use reifydb_type::value::r#type::Type;

pub(crate) mod series {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAMESPACE: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const TAG: usize = 3;
	pub(crate) const PRECISION: usize = 4;
	pub(crate) const PRIMARY_KEY: usize = 5;

	pub(crate) static SCHEMA: Lazy<Schema> = Lazy::new(|| {
		Schema::new(vec![
			SchemaField::unconstrained("id", Type::Uint8),
			SchemaField::unconstrained("namespace", Type::Uint8),
			SchemaField::unconstrained("name", Type::Utf8),
			SchemaField::unconstrained("tag", Type::Uint8),
			SchemaField::unconstrained("precision", Type::Uint1),
			SchemaField::unconstrained("primary_key", Type::Uint8),
		])
	});
}

pub(crate) mod series_namespace {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;

	pub(crate) static SCHEMA: Lazy<Schema> = Lazy::new(|| {
		Schema::new(vec![
			SchemaField::unconstrained("id", Type::Uint8),
			SchemaField::unconstrained("name", Type::Utf8),
		])
	});
}

pub(crate) mod series_metadata {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const ROW_COUNT: usize = 1;
	pub(crate) const OLDEST_TIMESTAMP: usize = 2;
	pub(crate) const NEWEST_TIMESTAMP: usize = 3;
	pub(crate) const SEQUENCE_COUNTER: usize = 4;

	pub(crate) static SCHEMA: Lazy<Schema> = Lazy::new(|| {
		Schema::new(vec![
			SchemaField::unconstrained("id", Type::Uint8),
			SchemaField::unconstrained("row_count", Type::Uint8),
			SchemaField::unconstrained("oldest_timestamp", Type::Int8),
			SchemaField::unconstrained("newest_timestamp", Type::Int8),
			SchemaField::unconstrained("sequence_counter", Type::Uint8),
		])
	});
}
