// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_core::{
	common::{CommitVersion, TimeSource},
	value::{batch::concat_columns, column::factory},
};
use reifydb_store_column::{
	compress::Compressor,
	snapshot::{ColumnBlock, ColumnChunks},
};
use reifydb_value::{
	Result, reifydb_assertions,
	value::{
		system_columns::{SystemColumn, created_at, partitions, row_numbers, system_column, time, updated_at},
		value_type::ValueType,
	},
};

use crate::column::error::SubStoreError;

pub fn system_column_schema(time: &TimeSource, partitioned: bool) -> Vec<(String, ValueType)> {
	SystemColumn::ALL
		.into_iter()
		.filter(|sc| *sc != SystemColumn::Time || carries_time(time))
		.filter(|sc| *sc != SystemColumn::Partitions || partitioned)
		.map(|sc| (sc.name().to_string(), sc.ty()))
		.collect()
}

fn carries_time(time: &TimeSource) -> bool {
	match time {
		TimeSource::None => false,
		TimeSource::Event {
			..
		}
		| TimeSource::Processing => true,
	}
}

pub fn column_block_from_batches(
	schema: Vec<(String, ValueType)>,
	batches: Vec<RecordBatch>,
	version: CommitVersion,
	compressor: &Compressor,
) -> Result<ColumnBlock> {
	let timed = schema.iter().any(|(name, _)| SystemColumn::from_name(name) == Some(SystemColumn::Time));
	let partitioned =
		schema.iter().any(|(name, _)| SystemColumn::from_name(name) == Some(SystemColumn::Partitions));
	for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
		let partitions_present = system_column(batch, SystemColumn::Partitions).is_some();
		if partitions_present != partitioned {
			return Err(SubStoreError::PartitionMismatch {
				partitioned,
				present: partitions_present,
			}
			.into());
		}
		let time_present = system_column(batch, SystemColumn::Time).is_some();
		if time_present != timed {
			return Err(SubStoreError::TimeMismatch {
				timed,
				present: time_present,
			}
			.into());
		}
	}

	let mut chunked: Vec<ColumnChunks> = Vec::with_capacity(schema.len());

	#[cfg(reifydb_assertions)]
	let mut block_rows: Option<usize> = None;

	for (name, ty) in &schema {
		let combined = match SystemColumn::from_name(name) {
			Some(sc) => system_column_buffer(sc, &batches, version)?,
			None => user_column_buffer(name, &batches)?,
		};
		reifydb_assertions! {
			let rows = combined.1.len();
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
		chunked.push(compressor.compress(ty.clone(), &combined)?);
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

fn user_column_buffer(name: &str, batches: &[RecordBatch]) -> Result<(FieldRef, ArrayRef)> {
	let mut parts: Vec<(FieldRef, ArrayRef)> = Vec::with_capacity(batches.len());
	for batch in batches {
		let (index, _) = batch.schema_ref().column_with_name(name).ok_or_else(|| {
			SubStoreError::MissingColumnInBatch {
				column: name.to_string(),
			}
		})?;
		parts.push((batch.schema_ref().fields()[index].clone(), batch.column(index).clone()));
	}
	if parts.is_empty() {
		return Err(SubStoreError::NoBatchesForMaterialization {
			column: name.to_string(),
		}
		.into());
	}
	concat_columns(&parts)
}

fn system_column_buffer(
	sc: SystemColumn,
	batches: &[RecordBatch],
	version: CommitVersion,
) -> Result<(FieldRef, ArrayRef)> {
	if batches.is_empty() {
		return Err(SubStoreError::NoBatchesForMaterialization {
			column: sc.name().to_string(),
		}
		.into());
	}
	match sc {
		SystemColumn::RowNumbers => {
			let mut values = Vec::new();
			for batch in batches {
				for rn in row_numbers(batch)?.iter() {
					values.push(rn.0);
				}
			}
			Ok(factory::uint8(sc.name(), values))
		}
		SystemColumn::Partitions => {
			let mut values = Vec::new();
			for batch in batches {
				for partition in partitions(batch)?.iter() {
					values.push(partition.0);
				}
			}
			Ok(factory::uint16(sc.name(), values))
		}
		SystemColumn::CreatedAt => {
			let mut values = Vec::new();
			for batch in batches {
				for ts in created_at(batch)?.iter() {
					values.push(*ts);
				}
			}
			Ok(factory::datetime(sc.name(), values))
		}
		SystemColumn::UpdatedAt => {
			let mut values = Vec::new();
			for batch in batches {
				for ts in updated_at(batch)?.iter() {
					values.push(*ts);
				}
			}
			Ok(factory::datetime(sc.name(), values))
		}
		SystemColumn::Time => {
			let mut values = Vec::new();
			for batch in batches {
				for ts in time(batch)?.iter() {
					values.push(*ts);
				}
			}
			Ok(factory::datetime(sc.name(), values))
		}
		SystemColumn::CommitVersion => {
			let total: usize = batches.iter().map(|b| b.num_rows()).sum();
			Ok(factory::uint8(sc.name(), vec![version.0; total]))
		}
	}
}
