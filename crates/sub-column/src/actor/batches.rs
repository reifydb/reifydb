// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_column::{
	compress::Compressor,
	snapshot::{ColumnBlock, ColumnChunks},
};
use reifydb_core::value::column::{array::canonical::Canonical, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::{Result, value::r#type::Type};

use crate::error::SubColumnError;

pub fn column_block_from_batches(
	schema: Vec<(String, Type)>,
	batches: Vec<Columns>,
	compressor: &Compressor,
) -> Result<ColumnBlock> {
	let mut chunked: Vec<ColumnChunks> = Vec::with_capacity(schema.len());

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
