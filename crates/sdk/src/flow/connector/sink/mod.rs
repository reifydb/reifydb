// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod extern_c;

use reifydb_core::value::column::columns::Columns;
use reifydb_value::config::Config;

use crate::{error::Result, flow::operator::column::operator::OperatorColumn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SinkDiffType {
	Insert = 1,
	Update = 2,
	Remove = 3,
}

#[derive(Debug)]
pub struct SinkRecord {
	pub op: SinkDiffType,

	pub columns: Columns,
}

pub trait InProcessSinkMetadata {
	const NAME: &'static str;

	const VERSION: &'static str;

	const DESCRIPTION: &'static str;

	const INPUT_COLUMNS: &'static [OperatorColumn];
}

pub trait InProcessSink: Send + 'static {
	fn new(config: &Config) -> Result<Self>
	where
		Self: Sized;

	fn write(&mut self, records: &[SinkRecord]) -> Result<()>;

	fn shutdown(&mut self) -> Result<()>;
}

pub trait InProcessSinkWithMetadata: InProcessSink + InProcessSinkMetadata {}
impl<T> InProcessSinkWithMetadata for T where T: InProcessSink + InProcessSinkMetadata {}
