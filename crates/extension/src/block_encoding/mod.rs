// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod error;
pub mod registry;

use arrow_array::RecordBatch;
use arrow_schema::FieldRef;
use reifydb_value::{error::ValueError, value::Value};

pub trait BlockEncoding: Send + Sync {
	fn version(&self) -> u32;

	fn bind(
		&self,
		user: &[FieldRef],
		config: &[(String, Value)],
	) -> Result<Box<dyn BoundBlockEncoding>, ValueError>;
}

pub trait BoundBlockEncoding: Send + Sync {
	fn stored_fields(&self) -> Vec<FieldRef>;

	fn encode(&self, user: RecordBatch) -> Result<RecordBatch, ValueError>;

	fn decode(&self, version: u32, stored: RecordBatch) -> Result<RecordBatch, ValueError>;
}
