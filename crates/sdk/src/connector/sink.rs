// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Sink connector traits for exporting data to external systems

use std::collections::HashMap;

use reifydb_core::value::column::columns::Columns;
use reifydb_type::value::Value;

use crate::{error::Result, operator::column::OperatorColumn};

/// The type of change operation for a sink record
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SinkDiffType {
	Insert = 1,
	Update = 2,
	Remove = 3,
}

/// A single record delivered to a sink connector
#[derive(Debug)]
pub struct SinkRecord {
	/// The type of change (insert, update, or remove)
	pub op: SinkDiffType,
	/// The columnar data (post-values for insert/update, pre-values for remove)
	pub columns: Columns,
}

/// Static metadata about a sink connector type
pub trait FFISinkMetadata {
	/// Connector name (e.g., "postgres", "kafka", "http")
	const NAME: &'static str;
	/// Semantic version (e.g., "1.0.0")
	const VERSION: &'static str;
	/// Human-readable description
	const DESCRIPTION: &'static str;
	/// Shape of records this sink accepts
	const INPUT_COLUMNS: &'static [OperatorColumn];
}

/// Runtime behavior of a sink connector
pub trait FFISink: Send + 'static {
	/// Create a new sink instance from config
	fn new(config: &HashMap<String, Value>) -> Result<Self>
	where
		Self: Sized;

	/// Write a batch of records to the external system.
	/// Must be idempotent for at-least-once delivery semantics.
	fn write(&mut self, records: &[SinkRecord]) -> Result<()>;

	/// Graceful shutdown
	fn shutdown(&mut self) -> Result<()>;
}

/// Blanket trait combining metadata and runtime behavior
pub trait FFISinkWithMetadata: FFISink + FFISinkMetadata {}
impl<T> FFISinkWithMetadata for T where T: FFISink + FFISinkMetadata {}
