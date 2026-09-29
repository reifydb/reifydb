// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use reifydb_core::{interface::catalog::column::Column, value::column::builder::ColumnBuilder};

pub mod ringbuffer;
pub mod series;
pub mod table;
pub mod view;

pub(crate) fn catalog_schema(columns: &[Column]) -> SchemaRef {
	Arc::new(Schema::new(
		columns.iter()
			.map(|column| {
				ColumnBuilder::with_capacity(column.constraint.get_type(), 0).finish(&column.name).0
			})
			.collect::<Vec<_>>(),
	))
}
