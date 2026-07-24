// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_catalog::vtable::user::UserVTableColumn;
use reifydb_core::{
	interface::catalog::id::NamespaceId,
	lifecycle::metrics::RetentionMetrics,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_value::{
	fragment::Fragment,
	value::{datetime::DateTime, value_type::ValueType},
};

use crate::framework::source::MetricsSource;

pub struct LifecycleSource {
	metrics: RetentionMetrics,
}

impl LifecycleSource {
	pub fn new(metrics: RetentionMetrics) -> Self {
		Self {
			metrics,
		}
	}
}

fn optional(name: &str, data_type: ValueType) -> UserVTableColumn {
	UserVTableColumn {
		name: name.to_string(),
		data_type,
		undefined: true,
	}
}

impl MetricsSource for LifecycleSource {
	fn namespace(&self) -> NamespaceId {
		NamespaceId::SYSTEM_METRICS_LIFECYCLE
	}

	fn columns(&self) -> Vec<UserVTableColumn> {
		vec![
			UserVTableColumn::new("ts", ValueType::DateTime),
			UserVTableColumn::new("class", ValueType::Utf8),
			UserVTableColumn::new("floor_version", ValueType::Uint8),
			optional("binding", ValueType::Utf8),
			UserVTableColumn::new("work_done", ValueType::Uint8),
			UserVTableColumn::new("backlog_hint", ValueType::Uint8),
			UserVTableColumn::new("slices", ValueType::Uint8),
			UserVTableColumn::new("stuck_slices", ValueType::Uint8),
			UserVTableColumn::new("budget_exhausted_slices", ValueType::Uint8),
			UserVTableColumn::new("gated_slices", ValueType::Uint8),
			optional("freelist_pages", ValueType::Uint8),
			optional("page_count", ValueType::Uint8),
		]
	}

	fn collect(&self, now: DateTime) -> Columns {
		let report = self.metrics.report();
		let capacity = report.len();

		let mut ts = ColumnBuffer::datetime_with_capacity(capacity);
		let mut class = Vec::with_capacity(capacity);
		let mut floor_version = ColumnBuffer::uint8_with_capacity(capacity);
		let mut binding = Vec::with_capacity(capacity);
		let mut work_done = ColumnBuffer::uint8_with_capacity(capacity);
		let mut backlog_hint = ColumnBuffer::uint8_with_capacity(capacity);
		let mut slices = ColumnBuffer::uint8_with_capacity(capacity);
		let mut stuck_slices = ColumnBuffer::uint8_with_capacity(capacity);
		let mut budget_exhausted_slices = ColumnBuffer::uint8_with_capacity(capacity);
		let mut gated_slices = ColumnBuffer::uint8_with_capacity(capacity);
		let mut freelist_pages = Vec::with_capacity(capacity);
		let mut page_count = Vec::with_capacity(capacity);

		for (retention_class, snapshot) in report {
			ts.push(now);
			class.push(retention_class.name().to_string());
			floor_version.push(snapshot.floor_version);
			binding.push(snapshot.binding.map(|term| term.to_string()));
			work_done.push(snapshot.work_done);
			backlog_hint.push(snapshot.backlog_hint);
			slices.push(snapshot.slices);
			stuck_slices.push(snapshot.stuck_slices);
			budget_exhausted_slices.push(snapshot.budget_exhausted_slices);
			gated_slices.push(snapshot.gated_slices);
			freelist_pages.push(snapshot.freelist.map(|gauge| gauge.freelist_pages));
			page_count.push(snapshot.freelist.map(|gauge| gauge.page_count));
		}

		Columns::new(vec![
			ColumnWithName::new(Fragment::internal("ts"), ts),
			ColumnWithName::new(Fragment::internal("class"), ColumnBuffer::utf8(class)),
			ColumnWithName::new(Fragment::internal("floor_version"), floor_version),
			ColumnWithName::new(Fragment::internal("binding"), ColumnBuffer::utf8_optional(binding)),
			ColumnWithName::new(Fragment::internal("work_done"), work_done),
			ColumnWithName::new(Fragment::internal("backlog_hint"), backlog_hint),
			ColumnWithName::new(Fragment::internal("slices"), slices),
			ColumnWithName::new(Fragment::internal("stuck_slices"), stuck_slices),
			ColumnWithName::new(Fragment::internal("budget_exhausted_slices"), budget_exhausted_slices),
			ColumnWithName::new(Fragment::internal("gated_slices"), gated_slices),
			ColumnWithName::new(
				Fragment::internal("freelist_pages"),
				ColumnBuffer::uint8_optional(freelist_pages),
			),
			ColumnWithName::new(Fragment::internal("page_count"), ColumnBuffer::uint8_optional(page_count)),
		])
	}
}

pub fn lifecycle_sources(metrics: &RetentionMetrics) -> Vec<Arc<dyn MetricsSource>> {
	vec![Arc::new(LifecycleSource::new(metrics.clone())) as Arc<dyn MetricsSource>]
}
