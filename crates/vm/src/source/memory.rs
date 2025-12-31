// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::Columns;

use crate::{error::Result, pipeline::Pipeline};

/// In-memory data source that yields a single batch.
pub struct InMemorySource {
	data: Option<Columns>,
}

impl InMemorySource {
	/// Create an in-memory source from a Columns batch.
	pub fn new(data: Columns) -> Self {
		Self {
			data: Some(data),
		}
	}

	/// Convert this source into a Pipeline.
	pub fn into_pipeline(self) -> Pipeline {
		let data = self.data;
		Box::pin(futures_util::stream::once(
			async move { data.ok_or_else(|| crate::error::VmError::EmptyPipeline) },
		))
	}
}

/// Create a pipeline from a Columns batch.
pub fn from_columns(data: Columns) -> Pipeline {
	InMemorySource::new(data).into_pipeline()
}

/// Create an empty pipeline that yields no batches.
pub fn empty() -> Pipeline {
	Box::pin(futures_util::stream::empty())
}

/// Create a pipeline from multiple batches.
pub fn from_batches(batches: Vec<Columns>) -> Pipeline {
	Box::pin(futures_util::stream::iter(batches.into_iter().map(Ok::<_, crate::error::VmError>)))
}

/// Create a pipeline from a single result.
pub fn from_result(result: Result<Columns>) -> Pipeline {
	Box::pin(futures_util::stream::once(async move { result }))
}
