// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB


use reifydb_transaction::transaction::Transaction;
use tracing::{Span, field, instrument};

use super::RowShapeRegistry;
use crate::{Result, store::row_shape::find::load_all_row_shapes};

/// Loads shapes from storage into the RowShapeRegistry cache.
pub struct RowShapeRegistryLoader;

impl RowShapeRegistryLoader {
	/// Load all shapes from storage into the registry cache.
	///
	/// This is called during database startup to populate the cache
	/// with persisted shapes.
	#[instrument(
		name = "row_shape_registry::load_all",
		level = "debug",
		skip(rx, registry),
		fields(shape_count = field::Empty)
	)]
	pub fn load_all(rx: &mut Transaction<'_>, registry: &RowShapeRegistry) -> Result<()> {
		let shapes = load_all_row_shapes(rx)?;

		Span::current().record("shape_count", shapes.len());

		for shape in shapes {
			registry.cache_row_shape(shape);
		}

		Ok(())
	}
}
