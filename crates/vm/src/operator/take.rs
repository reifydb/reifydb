// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	pin::Pin,
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
	task::{Context, Poll},
};

use futures_util::Stream;
use pin_project::pin_project;
use reifydb_core::value::column::Columns;

use crate::{error::Result, pipeline::Pipeline};

/// Take operator - limits the number of rows returned.
pub struct TakeOp {
	pub limit: usize,
}

impl TakeOp {
	pub fn new(limit: usize) -> Self {
		Self {
			limit,
		}
	}

	pub fn apply(&self, input: Pipeline) -> Pipeline {
		Box::pin(TakeStream {
			input,
			remaining: Arc::new(AtomicUsize::new(self.limit)),
		})
	}
}

#[pin_project]
struct TakeStream {
	#[pin]
	input: Pipeline,
	remaining: Arc<AtomicUsize>,
}

impl Stream for TakeStream {
	type Item = Result<Columns>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		let this = self.project();

		// Check if we've already taken enough
		let remaining = this.remaining.load(Ordering::SeqCst);
		if remaining == 0 {
			return Poll::Ready(None);
		}

		match this.input.poll_next(cx) {
			Poll::Pending => Poll::Pending,
			Poll::Ready(None) => Poll::Ready(None),
			Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
			Poll::Ready(Some(Ok(batch))) => {
				let batch_size = batch.row_count();

				if batch_size <= remaining {
					// Take entire batch
					this.remaining.fetch_sub(batch_size, Ordering::SeqCst);
					Poll::Ready(Some(Ok(batch)))
				} else {
					// Take partial batch
					this.remaining.store(0, Ordering::SeqCst);

					// Extract only the first `remaining` rows
					let indices: Vec<usize> = (0..remaining).collect();
					let truncated = batch.extract_by_indices(&indices);
					Poll::Ready(Some(Ok(truncated)))
				}
			}
		}
	}
}
