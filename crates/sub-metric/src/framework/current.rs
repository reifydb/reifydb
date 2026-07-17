// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_catalog::vtable::user::{UserVTable, UserVTableColumn};
use reifydb_core::value::column::columns::Columns;
use reifydb_runtime::context::clock::Clock;
use reifydb_value::value::datetime::DateTime;

use crate::framework::source::MetricSource;

#[derive(Clone)]
pub struct CurrentVTable {
	source: Arc<dyn MetricSource>,
	clock: Clock,
}

impl CurrentVTable {
	pub fn new(source: Arc<dyn MetricSource>, clock: Clock) -> Self {
		Self {
			source,
			clock,
		}
	}
}

impl UserVTable for CurrentVTable {
	fn vtable(&self) -> Vec<UserVTableColumn> {
		self.source.columns()
	}

	fn get(&self) -> Columns {
		self.source.collect(DateTime::from_nanos(self.clock.now_nanos()))
	}
}
