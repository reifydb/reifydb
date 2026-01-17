// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use once_cell::sync::Lazy;
use reifydb_core::encoded::schema::{Schema, SchemaField};
use reifydb_type::value::r#type::Type;

pub(crate) mod ringbuffer {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAMESPACE: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const CAPACITY: usize = 3;
	pub(crate) const PRIMARY_KEY: usize = 4;

	pub(crate) static SCHEMA: Lazy<Schema> = Lazy::new(|| {
		Schema::new(vec![
			SchemaField::unconstrained("id", Type::Uint8),
			SchemaField::unconstrained("namespace", Type::Uint8),
			SchemaField::unconstrained("name", Type::Utf8),
			SchemaField::unconstrained("capacity", Type::Uint8),
			SchemaField::unconstrained("primary_key", Type::Uint8),
		])
	});
}

pub(crate) mod ringbuffer_namespace {
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

pub(crate) mod ringbuffer_metadata {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const CAPACITY: usize = 1;
	pub(crate) const HEAD: usize = 2;
	pub(crate) const TAIL: usize = 3;
	pub(crate) const COUNT: usize = 4;

	pub(crate) static SCHEMA: Lazy<Schema> = Lazy::new(|| {
		Schema::new(vec![
			SchemaField::unconstrained("id", Type::Uint8),
			SchemaField::unconstrained("capacity", Type::Uint8),
			SchemaField::unconstrained("head", Type::Uint8),
			SchemaField::unconstrained("tail", Type::Uint8),
			SchemaField::unconstrained("count", Type::Uint8),
		])
	});
}
