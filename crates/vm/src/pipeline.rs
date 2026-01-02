// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::pin::Pin;

use futures_util::Stream;
use reifydb_core::{Batch, value::column::Columns};

use crate::error::{Result, VmError};

/// A pipeline is an async stream of batches.
/// Each batch can be lazy (encoded) or materialized (decoded).
/// This is the core data type that flows through all operators.
pub type Pipeline = Pin<Box<dyn Stream<Item = Result<Batch>> + Send>>;

/// Collect all batches from a pipeline into a single Columns.
/// Materializes all lazy batches and merges them together.
pub async fn collect(mut pipeline: Pipeline) -> Result<Columns> {
	use futures_util::StreamExt;

	let mut result: Option<Columns> = None;

	while let Some(batch_result) = pipeline.next().await {
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
	Box::pin(futures_util::stream::once(async move { Ok(batch) }))
}

/// Create a pipeline from a Columns batch (wrapped as materialized).
pub fn from_columns(data: Columns) -> Pipeline {
	Box::pin(futures_util::stream::once(async move { Ok(Batch::fully_materialized(data)) }))
}

/// Create an empty pipeline that yields no batches.
pub fn empty() -> Pipeline {
	Box::pin(futures_util::stream::empty())
}

/// Create a pipeline from multiple batches.
pub fn from_batches(batches: Vec<Batch>) -> Pipeline {
	Box::pin(futures_util::stream::iter(batches.into_iter().map(Ok::<_, VmError>)))
}

/// Create a pipeline from a single result.
pub fn from_result(result: Result<Batch>) -> Pipeline {
	Box::pin(futures_util::stream::once(async move { result }))
}
