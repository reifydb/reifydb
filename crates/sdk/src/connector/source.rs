// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Source connector traits for ingesting data from external systems

use std::{collections::HashMap, sync::mpsc::SyncSender};

use reifydb_core::value::column::columns::Columns;
use reifydb_type::value::Value;

use crate::{
	error::{FFIError, Result},
	operator::column::OperatorColumn,
};

/// Whether a source connector operates in pull or push mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceMode {
	/// Source is polled on an interval (e.g., database query, S3 scan)
	Pull,
	/// Source runs continuously and pushes records (e.g., Kafka consumer, MQTT subscriber)
	Push,
}

/// A batch of records produced by a source connector
#[derive(Debug)]
pub struct SourceBatch {
	/// The columnar data
	pub columns: Columns,
	/// Opaque checkpoint for resumption - format is connector-defined
	pub checkpoint: Option<Vec<u8>>,
}

impl SourceBatch {
	pub fn empty() -> Self {
		Self {
			columns: Columns::empty(),
			checkpoint: None,
		}
	}

	pub fn is_empty(&self) -> bool {
		self.columns.is_empty()
	}
}

/// Static metadata about a source connector type
pub trait FFISourceMetadata {
	/// Connector name (e.g., "postgres", "kafka", "mqtt")
	const NAME: &'static str;
	/// Semantic version (e.g., "1.0.0")
	const VERSION: &'static str;
	/// Human-readable description
	const DESCRIPTION: &'static str;
	/// Pull or Push mode
	const MODE: SourceMode;
	/// Shape of records this source produces
	const OUTPUT_COLUMNS: &'static [OperatorColumn];
}

/// Runtime behavior of a source connector
pub trait FFISource: Send + 'static {
	/// Create a new source instance from config
	fn new(config: &HashMap<String, Value>) -> Result<Self>
	where
		Self: Sized;

	/// Pull mode: fetch the next batch of records.
	/// Called on the configured poll interval.
	/// `checkpoint` is the last committed checkpoint (None on first poll).
	fn poll(&mut self, checkpoint: Option<&[u8]>) -> Result<SourceBatch>;

	/// Push mode: run continuously, calling `emitter.emit()` for each batch.
	/// Blocks until shutdown or error.
	fn run(&mut self, checkpoint: Option<&[u8]>, emitter: SourceEmitter) -> Result<()>;

	/// Graceful shutdown
	fn shutdown(&mut self) -> Result<()>;
}

/// Channel for push-mode sources to emit batches
pub struct SourceEmitter {
	sender: SyncSender<SourceBatch>,
}

impl SourceEmitter {
	pub fn new(sender: SyncSender<SourceBatch>) -> Self {
		Self {
			sender,
		}
	}

	/// Emit a batch of records. Blocks if the channel is full (backpressure).
	pub fn emit(&self, batch: SourceBatch) -> Result<()> {
		self.sender.send(batch).map_err(|_| FFIError::Other("source emitter channel closed".to_string()))
	}
}

/// Blanket trait combining metadata and runtime behavior
pub trait FFISourceWithMetadata: FFISource + FFISourceMetadata {}
impl<T> FFISourceWithMetadata for T where T: FFISource + FFISourceMetadata {}
