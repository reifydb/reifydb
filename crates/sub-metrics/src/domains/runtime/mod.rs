// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod collect;

use std::sync::Arc;

use collect::{Collectors, collect_memory, collect_operators, collect_watermarks};
use reifydb_catalog::vtable::user::UserVTableColumn;
use reifydb_core::{
	interface::catalog::id::NamespaceId,
	metrics::sample::MetricsSample,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_value::{
	fragment::Fragment,
	value::{datetime::DateTime, value_type::ValueType},
};

use crate::framework::source::MetricsSource;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Domain {
	Memory,
	Watermarks,
	Operators,
}

impl Domain {
	pub const ALL: [Domain; 3] = [Domain::Memory, Domain::Watermarks, Domain::Operators];

	pub fn namespace(&self) -> NamespaceId {
		match self {
			Domain::Memory => NamespaceId::SYSTEM_METRICS_RUNTIME_MEMORY,
			Domain::Watermarks => NamespaceId::SYSTEM_METRICS_RUNTIME_WATERMARKS,
			Domain::Operators => NamespaceId::SYSTEM_METRICS_RUNTIME_OPERATORS,
		}
	}

	pub fn local_name(&self) -> &'static str {
		match self {
			Domain::Memory => "memory",
			Domain::Watermarks => "watermarks",
			Domain::Operators => "operators",
		}
	}

	pub fn collect(&self, c: &Collectors) -> Vec<MetricsSample> {
		match self {
			Domain::Memory => collect_memory(c),
			Domain::Watermarks => collect_watermarks(c),
			Domain::Operators => collect_operators(c),
		}
	}
}

pub fn runtime_columns() -> Vec<UserVTableColumn> {
	vec![
		UserVTableColumn::new("ts", ValueType::DateTime),
		UserVTableColumn::new("scope", ValueType::Utf8),
		UserVTableColumn::new("metric", ValueType::Utf8),
		UserVTableColumn::new("value", ValueType::Float8),
		UserVTableColumn::new("unit", ValueType::Utf8),
	]
}

pub(crate) fn samples_to_columns(samples: &[MetricsSample], now: DateTime) -> Columns {
	let capacity = samples.len();
	let mut ts = ColumnBuffer::datetime_with_capacity(capacity);
	let mut scope = ColumnBuffer::utf8_with_capacity(capacity);
	let mut metric = ColumnBuffer::utf8_with_capacity(capacity);
	let mut value = ColumnBuffer::float8_with_capacity(capacity);
	let mut unit = ColumnBuffer::utf8_with_capacity(capacity);

	for s in samples {
		ts.push(now);
		scope.push(s.scope.as_ref());
		metric.push(s.metric);
		value.push(s.reading.as_f64());
		unit.push(s.reading.unit());
	}

	Columns::new(vec![
		ColumnWithName::new(Fragment::internal("ts"), ts),
		ColumnWithName::new(Fragment::internal("scope"), scope),
		ColumnWithName::new(Fragment::internal("metric"), metric),
		ColumnWithName::new(Fragment::internal("value"), value),
		ColumnWithName::new(Fragment::internal("unit"), unit),
	])
}

pub struct RuntimeSource {
	domain: Domain,
	collectors: Collectors,
}

impl MetricsSource for RuntimeSource {
	fn namespace(&self) -> NamespaceId {
		self.domain.namespace()
	}

	fn columns(&self) -> Vec<UserVTableColumn> {
		runtime_columns()
	}

	fn collect(&self, now: DateTime) -> Columns {
		samples_to_columns(&self.domain.collect(&self.collectors), now)
	}
}

pub fn runtime_source(domain: Domain, collectors: &Collectors) -> Arc<dyn MetricsSource> {
	Arc::new(RuntimeSource {
		domain,
		collectors: collectors.clone(),
	}) as Arc<dyn MetricsSource>
}

#[derive(Clone)]
pub struct SampleReader {
	collectors: Collectors,
}

impl SampleReader {
	pub fn new(collectors: Collectors) -> Self {
		Self {
			collectors,
		}
	}

	pub fn samples_for(&self, domain: Domain) -> Vec<MetricsSample> {
		domain.collect(&self.collectors)
	}
}
