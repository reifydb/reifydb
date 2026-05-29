// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::vtable::user::{UserVTable, UserVTableColumn};
use reifydb_core::{
	profiler::ProfilerCategoryId,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_value::{fragment::Fragment, value::value_type::ValueType};

use crate::profiler_gauges::{CategoryGauges, gauges_for};

#[derive(Clone)]
pub struct MetricsProfilerCategoriesVTable;

impl MetricsProfilerCategoriesVTable {
	pub fn new() -> Self {
		Self
	}

	pub fn columns_spec() -> Vec<UserVTableColumn> {
		vec![
			UserVTableColumn::new("category", ValueType::Utf8),
			UserVTableColumn::new("calls", ValueType::Uint8),
			UserVTableColumn::new("p50_us", ValueType::Uint8),
			UserVTableColumn::new("p75_us", ValueType::Uint8),
			UserVTableColumn::new("p90_us", ValueType::Uint8),
			UserVTableColumn::new("p95_us", ValueType::Uint8),
			UserVTableColumn::new("p99_us", ValueType::Uint8),
		]
	}
}

impl Default for MetricsProfilerCategoriesVTable {
	fn default() -> Self {
		Self::new()
	}
}

const CATEGORY_NAMES: [&str; 6] = ["query", "txn", "storage", "plan", "cdc", "flow"];

impl UserVTable for MetricsProfilerCategoriesVTable {
	fn vtable(&self) -> Vec<UserVTableColumn> {
		Self::columns_spec()
	}

	fn get(&self) -> Columns {
		let capacity = CATEGORY_NAMES.len();
		let mut category = ColumnBuffer::utf8_with_capacity(capacity);
		let mut calls = ColumnBuffer::uint8_with_capacity(capacity);
		let mut p50 = ColumnBuffer::uint8_with_capacity(capacity);
		let mut p75 = ColumnBuffer::uint8_with_capacity(capacity);
		let mut p90 = ColumnBuffer::uint8_with_capacity(capacity);
		let mut p95 = ColumnBuffer::uint8_with_capacity(capacity);
		let mut p99 = ColumnBuffer::uint8_with_capacity(capacity);

		for (idx, name) in CATEGORY_NAMES.iter().enumerate() {
			let cat_id = ProfilerCategoryId(idx as u8);
			let g: &CategoryGauges = gauges_for(cat_id).expect("six categories with static gauges");
			category.push(*name);
			calls.push(g.calls.get() as u64);
			p50.push(g.p50.get() as u64);
			p75.push(g.p75.get() as u64);
			p90.push(g.p90.get() as u64);
			p95.push(g.p95.get() as u64);
			p99.push(g.p99.get() as u64);
		}

		Columns::new(vec![
			ColumnWithName::new(Fragment::internal("category"), category),
			ColumnWithName::new(Fragment::internal("calls"), calls),
			ColumnWithName::new(Fragment::internal("p50_us"), p50),
			ColumnWithName::new(Fragment::internal("p75_us"), p75),
			ColumnWithName::new(Fragment::internal("p90_us"), p90),
			ColumnWithName::new(Fragment::internal("p95_us"), p95),
			ColumnWithName::new(Fragment::internal("p99_us"), p99),
		])
	}
}
