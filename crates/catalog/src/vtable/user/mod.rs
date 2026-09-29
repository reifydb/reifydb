// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod registry;

use arrow_array::RecordBatch;
use reifydb_core::metrics::sample::MetricKind;
use reifydb_value::value::value_type::ValueType;

#[derive(Debug, Clone)]
pub struct UserVTableColumn {
	pub name: String,

	pub data_type: ValueType,

	pub undefined: bool,

	pub kind: MetricKind,
}

impl UserVTableColumn {
	pub fn new(name: impl Into<String>, data_type: ValueType) -> Self {
		Self {
			name: name.into(),
			data_type,
			undefined: false,
			kind: MetricKind::Dimension,
		}
	}

	pub fn measure(name: impl Into<String>, data_type: ValueType, kind: MetricKind) -> Self {
		Self {
			name: name.into(),
			data_type,
			undefined: false,
			kind,
		}
	}
}

pub trait UserVTable: Clone + Send + Sync + 'static {
	fn vtable(&self) -> Vec<UserVTableColumn>;

	fn get(&self) -> RecordBatch;
}
