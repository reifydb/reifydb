// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_column::{
	array::canonical::CanonicalArray, chunked::ChunkedArray, column_block::ColumnBlock, compress::Compressor,
};
use reifydb_core::value::column::{columns::Columns, data::ColumnData};
use reifydb_type::{Result, error::Error, value::r#type::Type};
use serde::de::Error as _;

// Concatenate a sequence of scan-emitted `Columns` batches into a single-chunk
// `ColumnBlock` aligned to `schema`. Layout: for each `(name, ty)` in schema,
// collect the column's `ColumnData` from every batch, extend into one combined
// `ColumnData`, then `CanonicalArray::from_column_data → Compressor::compress →
// ChunkedArray::single`. Output columns appear in the order given by `schema`,
// not the scan's emission order.
//
// `schema` uses `(String, Type)` because nullability is derived from the
// resulting `CanonicalArray.nullable` (which in turn reflects whether the
// underlying `ColumnData` was wrapped as `Option { inner, bitvec }`).
pub fn column_block_from_batches(
	schema: Vec<(String, Type)>,
	batches: Vec<Columns>,
	compressor: &Compressor,
) -> Result<ColumnBlock> {
	let mut chunked: Vec<ChunkedArray> = Vec::with_capacity(schema.len());

	for (name, ty) in &schema {
		let mut combined: Option<ColumnData> = None;
		for batch in &batches {
			let column = batch.iter().find(|c| c.name().text() == name.as_str()).ok_or_else(|| {
				Error::custom(format!("column_block_from_batches: scan output missing column '{name}'"))
			})?;
			let data = column.data.clone();
			match combined.as_mut() {
				None => combined = Some(data),
				Some(acc) => acc.extend(data)?,
			}
		}
		let data = combined.ok_or_else(|| {
			Error::custom(format!("column_block_from_batches: no batches to materialize column '{name}'"))
		})?;
		let canonical = CanonicalArray::from_column_data(&data)?;
		let nullable = canonical.nullable;
		let array = compressor.compress(&canonical)?;
		chunked.push(ChunkedArray::single(ty.clone(), nullable, array));
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
