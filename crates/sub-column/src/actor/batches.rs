// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_column::{
	compress::Compressor,
	snapshot::{ColumnBlock, ColumnChunks},
};
use reifydb_core::value::column::{buffer::ColumnBuffer, columns::Columns, data::canonical::Canonical};
use reifydb_value::{Result, value::value_type::ValueType};

use crate::error::SubColumnError;

pub fn column_block_from_batches(
	schema: Vec<(String, ValueType)>,
	batches: Vec<Columns>,
	compressor: &Compressor,
) -> Result<ColumnBlock> {
	let mut chunked: Vec<ColumnChunks> = Vec::with_capacity(schema.len());

	#[cfg(reifydb_assertions)]
	let mut block_rows: Option<usize> = None;

	for (name, ty) in &schema {
		let mut combined: Option<ColumnBuffer> = None;
		for batch in &batches {
			let column = batch.iter().find(|c| c.name().text() == name.as_str()).ok_or_else(|| {
				SubColumnError::MissingColumnInBatch {
					column: name.clone(),
				}
			})?;
			let data = column.data().clone();
			match combined.as_mut() {
				None => combined = Some(data),
				Some(acc) => acc.extend(data)?,
			}
		}
		let data = combined.ok_or_else(|| SubColumnError::NoBatchesForMaterialization {
			column: name.clone(),
		})?;
		let canonical = Canonical::from_column_buffer(&data)?;
		#[cfg(reifydb_assertions)]
		{
			let rows = canonical.len();
			match block_rows {
				None => block_rows = Some(rows),
				Some(expected) => assert!(
					rows == expected,
					"sub-column assembled a ragged column block: column '{}' has {} rows but earlier columns have {}, so a row-wise read of the block would misalign fields or index past a shorter column",
					name,
					rows,
					expected
				),
			}
		}
		let nullable = canonical.nullable;
		let array = compressor.compress(&canonical)?;
		chunked.push(ColumnChunks::single(ty.clone(), nullable, array));
	}

	let schema_arc = Arc::new(
		schema.into_iter()
			.enumerate()
			.map(|(i, (name, ty))| {
				let nullable = chunked[i].nullable;
				(name, ty, nullable)
			})
			.collect::<Vec<_>>(),
	);
	Ok(ColumnBlock::new(schema_arc, chunked))
}
