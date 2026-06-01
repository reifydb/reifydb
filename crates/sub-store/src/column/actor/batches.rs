// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_column::{
	compress::Compressor,
	snapshot::{ColumnBlock, ColumnChunks, SystemColumn},
};
use reifydb_core::value::column::{buffer::ColumnBuffer, columns::Columns, data::canonical::Canonical};
use reifydb_runtime::reifydb_assertions;
use reifydb_value::{Result, value::value_type::ValueType};

use crate::column::error::SubStoreError;

pub fn column_block_from_batches(
	schema: Vec<(String, ValueType)>,
	batches: Vec<Columns>,
	compressor: &Compressor,
) -> Result<ColumnBlock> {
	let mut chunked: Vec<ColumnChunks> = Vec::with_capacity(schema.len());

	#[cfg(reifydb_assertions)]
	let mut block_rows: Option<usize> = None;

	for (name, ty) in &schema {
		let combined = match SystemColumn::from_name(name) {
			Some(sc) => system_column_buffer(sc, &batches)?,
			None => user_column_buffer(name, &batches)?,
		};
		let canonical = Canonical::from_column_buffer(&combined)?;
		reifydb_assertions! {
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

fn user_column_buffer(name: &str, batches: &[Columns]) -> Result<ColumnBuffer> {
	let mut combined: Option<ColumnBuffer> = None;
	for batch in batches {
		let column = batch.iter().find(|c| c.name().text() == name).ok_or_else(|| {
			SubStoreError::MissingColumnInBatch {
				column: name.to_string(),
			}
		})?;
		let data = column.data().clone();
		match combined.as_mut() {
			None => combined = Some(data),
			Some(acc) => acc.extend(data)?,
		}
	}
	combined.ok_or_else(|| {
		SubStoreError::NoBatchesForMaterialization {
			column: name.to_string(),
		}
		.into()
	})
}

fn system_column_buffer(sc: SystemColumn, batches: &[Columns]) -> Result<ColumnBuffer> {
	if batches.is_empty() {
		return Err(SubStoreError::NoBatchesForMaterialization {
			column: sc.name().to_string(),
		}
		.into());
	}
	match sc {
		SystemColumn::RowNumber => {
			let total: usize = batches.iter().map(|b| b.row_numbers.len()).sum();
			let mut values = Vec::with_capacity(total);
			for batch in batches {
				for rn in batch.row_numbers.iter() {
					values.push(rn.0);
				}
			}
			Ok(ColumnBuffer::uint8(values))
		}
		SystemColumn::CreatedAt => {
			let total: usize = batches.iter().map(|b| b.created_at.len()).sum();
			let mut values = Vec::with_capacity(total);
			for batch in batches {
				for ts in batch.created_at.iter() {
					values.push(*ts);
				}
			}
			Ok(ColumnBuffer::datetime(values))
		}
		SystemColumn::UpdatedAt => {
			let total: usize = batches.iter().map(|b| b.updated_at.len()).sum();
			let mut values = Vec::with_capacity(total);
			for batch in batches {
				for ts in batch.updated_at.iter() {
					values.push(*ts);
				}
			}
			Ok(ColumnBuffer::datetime(values))
		}
	}
}
