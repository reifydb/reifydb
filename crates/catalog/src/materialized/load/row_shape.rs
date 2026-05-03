// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::Transaction;
use tracing::{Span, field, instrument};

use crate::{Result, materialized::MaterializedCatalog, store::row_shape::find::load_all_row_shapes};

#[instrument(
	name = "materialized::load_row_shapes",
	level = "debug",
	skip(rx, catalog),
	fields(shape_count = field::Empty)
)]
pub(crate) fn load_row_shapes(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	let shapes = load_all_row_shapes(rx)?;

	Span::current().record("shape_count", shapes.len());

	for shape in shapes {
		catalog.set_row_shape(shape);
	}

	Ok(())
}
