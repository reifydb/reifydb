// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

//! Streaming result types for async query execution.
//!
//! This module provides types for streaming query results instead of
//! collecting them into `Vec<Frame>`. The primary type is `SendableFrameStream`,
//! which is a pinned, boxed, sendable stream of frames.

mod channel;
mod error;

use std::pin::Pin;

pub use channel::{ChannelFrameStream, FrameSender, StreamHandle, StreamId};
pub use error::{StreamError, StreamResult};
use futures_util::Stream;

use crate::Frame;

/// Primary result type for async query execution.
///
/// A sendable stream of query result frames. This is the async equivalent
/// of DataFusion's `SendableRecordBatchStream`, but uses `Frame` as the
/// data unit instead of `RecordBatch`.
///
/// The stream is bounded for backpressure - producers will wait if
/// consumers are slow.
pub type SendableFrameStream = Pin<Box<dyn Stream<Item = StreamResult<Frame>> + Send>>;

/// Configuration for streaming query execution.
#[derive(Debug, Clone)]
pub struct StreamConfig {
	/// Size of the bounded channel buffer (controls backpressure).
	/// Larger values use more memory but provide smoother throughput.
	pub buffer_size: usize,

	/// Batch size for each Frame (inherited from ExecutionContext).
	pub batch_size: u64,

	/// Optional timeout for the entire query in milliseconds.
	pub timeout_ms: Option<u64>,
}

impl Default for StreamConfig {
	fn default() -> Self {
		Self {
			buffer_size: 8,   // 8 frames in-flight
			batch_size: 1024, // 1024 rows per batch
			timeout_ms: None, // No timeout by default
		}
	}
}

/// Schema information for a frame stream.
#[derive(Debug, Clone)]
pub struct FrameSchema {
	/// Column names in order.
	pub column_names: Vec<String>,
	/// Column types (if known statically).
	pub column_types: Option<Vec<reifydb_type::Type>>,
}
