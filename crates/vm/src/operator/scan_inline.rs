// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Inline scan operator for creating pipelines from in-memory data.

use reifydb_core::value::column::Columns;

use crate::pipeline::Pipeline;

/// Operator for scanning in-memory data.
///
/// This is a source operator that creates a new pipeline from pre-materialized
/// columnar data. Primarily used for testing and temporary in-memory tables.
pub struct ScanInlineOp {
	pub data: Columns,
}

impl ScanInlineOp {
	/// Create a new inline scan operator from columnar data.
	pub fn new(data: Columns) -> Self {
		Self {
			data,
		}
	}

	/// Create a pipeline that yields the in-memory data as a single batch.
	pub fn create(&self) -> Pipeline {
		let data = self.data.clone();
		Box::pin(futures_util::stream::once(async move { Ok(data) }))
	}
}
