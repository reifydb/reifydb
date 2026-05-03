// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::value::column::columns::Columns;
use reifydb_type::value::Value;

use crate::{error::Result, operator::column::OperatorColumn};

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

pub trait FFISinkMetadata {
	const NAME: &'static str;

	const VERSION: &'static str;

	const DESCRIPTION: &'static str;

	const INPUT_COLUMNS: &'static [OperatorColumn];
}

pub trait FFISink: Send + 'static {
	fn new(config: &HashMap<String, Value>) -> Result<Self>
	where
		Self: Sized;

	fn write(&mut self, records: &[SinkRecord]) -> Result<()>;

	fn shutdown(&mut self) -> Result<()>;
}

pub trait FFISinkWithMetadata: FFISink + FFISinkMetadata {}
impl<T> FFISinkWithMetadata for T where T: FFISink + FFISinkMetadata {}
