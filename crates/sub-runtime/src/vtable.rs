// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::vtable::user::{UserVTable, UserVTableColumn};
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_runtime::context::clock::Clock;
use reifydb_value::{
	fragment::Fragment,
	value::{datetime::DateTime, value_type::ValueType},
};

use crate::{collect::Collectors, domain::Domain};

#[derive(Clone)]
pub struct RuntimeVTable {
	collectors: Collectors,
	clock: Clock,
	domain: Domain,
}

impl RuntimeVTable {
	pub fn new(collectors: Collectors, clock: Clock, domain: Domain) -> Self {
		Self {
			collectors,
			clock,
			domain,
		}
	}

	pub fn columns_spec() -> Vec<UserVTableColumn> {
		vec![
			UserVTableColumn::new("ts", ValueType::DateTime),
			UserVTableColumn::new("scope", ValueType::Utf8),
			UserVTableColumn::new("metric", ValueType::Utf8),
			UserVTableColumn::new("value", ValueType::Float8),
			UserVTableColumn::new("unit", ValueType::Utf8),
		]
	}
}

impl UserVTable for RuntimeVTable {
	fn vtable(&self) -> Vec<UserVTableColumn> {
		Self::columns_spec()
	}

	fn get(&self) -> Columns {
		let samples = self.domain.collect(&self.collectors);
		let now = DateTime::from_nanos(self.clock.now_nanos());
		let capacity = samples.len();

		let mut ts = ColumnBuffer::datetime_with_capacity(capacity);
		let mut scope = ColumnBuffer::utf8_with_capacity(capacity);
		let mut metric = ColumnBuffer::utf8_with_capacity(capacity);
		let mut value = ColumnBuffer::float8_with_capacity(capacity);
		let mut unit = ColumnBuffer::utf8_with_capacity(capacity);

		for s in &samples {
			ts.push(now);
			scope.push(s.scope);
			metric.push(s.metric);
			value.push(s.value);
			unit.push(s.unit);
		}

		Columns::new(vec![
			ColumnWithName::new(Fragment::internal("ts"), ts),
			ColumnWithName::new(Fragment::internal("scope"), scope),
			ColumnWithName::new(Fragment::internal("metric"), metric),
			ColumnWithName::new(Fragment::internal("value"), value),
			ColumnWithName::new(Fragment::internal("unit"), unit),
		])
	}
}
