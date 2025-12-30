// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::pin::Pin;

use futures_util::Stream;
use reifydb_core::value::column::Columns;

use crate::error::{Result, VmError};

/// A pipeline is an async stream of column batches.
/// This is the core data type that flows through all operators.
pub type Pipeline = Pin<Box<dyn Stream<Item = Result<Columns>> + Send>>;

/// Collect all batches from a pipeline into a single Columns.
pub async fn collect(mut pipeline: Pipeline) -> Result<Columns> {
	use futures_util::StreamExt;

	let mut result: Option<Columns> = None;

	while let Some(batch_result) = pipeline.next().await {
		let batch = batch_result?;

		if batch.row_count() == 0 {
			continue;
		}

		match &mut result {
			None => {
				result = Some(batch);
			}
			Some(existing) => {
				merge_columns(existing, batch)?;
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
