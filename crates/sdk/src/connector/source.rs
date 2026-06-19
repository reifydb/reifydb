// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::mpsc::SyncSender;

use reifydb_core::value::column::columns::Columns;

use crate::{
	config::Config,
	error::{Result, SdkError},
	operator::column::operator::OperatorColumn,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceMode {
	Pull,

	Push,
}

#[derive(Debug)]
pub struct SourceBatch {
	pub columns: Columns,

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
		!self.columns.has_rows()
	}
}

pub trait FFISourceMetadata {
	const NAME: &'static str;

	const VERSION: &'static str;

	const DESCRIPTION: &'static str;

	const MODE: SourceMode;

	const OUTPUT_COLUMNS: &'static [OperatorColumn];
}

pub trait FFISource: Send + 'static {
	fn new(config: &Config) -> Result<Self>
	where
		Self: Sized;

	fn poll(&mut self, checkpoint: Option<&[u8]>) -> Result<SourceBatch>;

	fn run(&mut self, checkpoint: Option<&[u8]>, emitter: SourceEmitter) -> Result<()>;

	fn shutdown(&mut self) -> Result<()>;
}

pub struct SourceEmitter {
	sender: SyncSender<SourceBatch>,
}

impl SourceEmitter {
	pub fn new(sender: SyncSender<SourceBatch>) -> Self {
		Self {
			sender,
		}
	}

	pub fn emit(&self, batch: SourceBatch) -> Result<()> {
		self.sender.send(batch).map_err(|_| SdkError::Other("source emitter channel closed".to_string()))
	}
}

pub trait FFISourceWithMetadata: FFISource + FFISourceMetadata {}
impl<T> FFISourceWithMetadata for T where T: FFISource + FFISourceMetadata {}
