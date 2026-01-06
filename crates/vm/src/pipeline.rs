// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{Batch, value::column::Columns};

use crate::error::{Result, VmError};

/// A pipeline is a sync iterator of batches.
/// Each batch can be lazy (encoded) or materialized (decoded).
/// This is the core data type that flows through all operators.
pub type Pipeline = Box<dyn Iterator<Item = Result<Batch>> + Send + Sync>;

/// Collect all batches from a pipeline into a single Columns.
/// Materializes all lazy batches and merges them together.
pub fn collect(mut pipeline: Pipeline) -> Result<Columns> {
	let mut result: Option<Columns> = None;

	while let Some(batch_result) = pipeline.next() {
		let batch = batch_result?;
		let columns = batch.into_columns(); // Materialize lazy batches

		if columns.row_count() == 0 {
			continue;
		}

		match &mut result {
			None => {
				result = Some(columns);
			}
			Some(existing) => {
				merge_columns(existing, columns)?;
			}
		}
	}

	Ok(result.unwrap_or_else(Columns::empty))
}

/// Merge source columns into target (in-place extension)
fn merge_columns(target: &mut Columns, source: Columns) -> Result<()> {
	if target.len() != source.len() {
		return Err(VmError::Internal(format!(
			"column count mismatch in merge: {} vs {}",
			target.len(),
			source.len()
		)));
	}

	// Extend row numbers
	target.row_numbers.extend(source.row_numbers.iter().cloned());

	// Extend each column
	for (i, src_col) in source.into_iter().enumerate() {
		target[i].extend(src_col).map_err(|e| VmError::Internal(e.to_string()))?;
	}

	Ok(())
}

/// Create a pipeline from a single Batch.
pub fn from_batch(batch: Batch) -> Pipeline {
	Box::new(std::iter::once(Ok(batch)))
}

/// Create a pipeline from a Columns batch (wrapped as materialized).
pub fn from_columns(data: Columns) -> Pipeline {
	Box::new(std::iter::once(Ok(Batch::fully_materialized(data))))
}

/// Create an empty pipeline that yields no batches.
pub fn empty() -> Pipeline {
	Box::new(std::iter::empty())
}

/// Create a pipeline from multiple batches.
pub fn from_batches(batches: Vec<Batch>) -> Pipeline {
	Box::new(batches.into_iter().map(Ok::<_, VmError>))
}

/// Create a pipeline from a single result.
pub fn from_result(result: Result<Batch>) -> Pipeline {
	Box::new(std::iter::once(result))
}
