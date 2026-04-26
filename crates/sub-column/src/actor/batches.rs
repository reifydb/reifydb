// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_column::{
	compress::Compressor,
	snapshot::{ColumnBlock, ColumnChunks, SystemColumn},
};
use reifydb_core::value::column::{buffer::ColumnBuffer, columns::Columns, data::canonical::Canonical};
use reifydb_type::{Result, value::r#type::Type};

use crate::error::SubColumnError;

pub fn column_block_from_batches(
	schema: Vec<(String, Type)>,
	batches: Vec<Columns>,
	compressor: &Compressor,
) -> Result<ColumnBlock> {
	let mut chunked: Vec<ColumnChunks> = Vec::with_capacity(schema.len());

	for (name, ty) in &schema {
		let combined = match SystemColumn::from_name(name) {
			Some(sc) => system_column_buffer(sc, &batches)?,
			None => user_column_buffer(name, &batches)?,
		};
		let canonical = Canonical::from_column_buffer(&combined)?;
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
			SubColumnError::MissingColumnInBatch {
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
		SubColumnError::NoBatchesForMaterialization {
			column: name.to_string(),
		}
		.into()
	})
}

fn system_column_buffer(sc: SystemColumn, batches: &[Columns]) -> Result<ColumnBuffer> {
	if batches.is_empty() {
		return Err(SubColumnError::NoBatchesForMaterialization {
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
