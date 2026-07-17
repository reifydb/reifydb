// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::vtable::user::{UserVTable, UserVTableColumn};
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_value::{fragment::Fragment, value::value_type::ValueType};

use super::reader::ProfilerReader;

#[derive(Clone)]
pub struct ProfilerSpansVTable {
	reader: ProfilerReader,
}

impl ProfilerSpansVTable {
	pub fn new(reader: ProfilerReader) -> Self {
		Self {
			reader,
		}
	}

	pub fn columns_spec() -> Vec<UserVTableColumn> {
		vec![
			UserVTableColumn::new("category", ValueType::Utf8),
			UserVTableColumn::new("span_name", ValueType::Utf8),
			UserVTableColumn::new("dim_1", ValueType::Utf8),
			UserVTableColumn::new("dim_2", ValueType::Utf8),
			UserVTableColumn::new("calls", ValueType::Uint8),
			UserVTableColumn::new("total", ValueType::Duration),
			UserVTableColumn::new("min", ValueType::Duration),
			UserVTableColumn::new("max", ValueType::Duration),
			UserVTableColumn::new("p50", ValueType::Duration),
			UserVTableColumn::new("p60", ValueType::Duration),
			UserVTableColumn::new("p70", ValueType::Duration),
			UserVTableColumn::new("p75", ValueType::Duration),
			UserVTableColumn::new("p80", ValueType::Duration),
			UserVTableColumn::new("p85", ValueType::Duration),
			UserVTableColumn::new("p90", ValueType::Duration),
			UserVTableColumn::new("p95", ValueType::Duration),
			UserVTableColumn::new("p98", ValueType::Duration),
			UserVTableColumn::new("p99", ValueType::Duration),
			UserVTableColumn::new("extra_0", ValueType::Uint8),
			UserVTableColumn::new("extra_1", ValueType::Uint8),
			UserVTableColumn::new("extra_2", ValueType::Uint8),
			UserVTableColumn::new("extra_3", ValueType::Uint8),
		]
	}
}

impl UserVTable for ProfilerSpansVTable {
	fn vtable(&self) -> Vec<UserVTableColumn> {
		Self::columns_spec()
	}

	fn get(&self) -> Columns {
		let mut records = self.reader.all();
		records.sort_by(|a, b| b.total_us.cmp(&a.total_us));
		let capacity = records.len();

		let mut category = ColumnBuffer::utf8_with_capacity(capacity);
		let mut span_names = ColumnBuffer::utf8_with_capacity(capacity);
		let mut dim_1 = ColumnBuffer::utf8_with_capacity(capacity);
		let mut dim_2 = ColumnBuffer::utf8_with_capacity(capacity);
		let mut calls = ColumnBuffer::uint8_with_capacity(capacity);
		let mut total = ColumnBuffer::duration_with_capacity(capacity);
		let mut min = ColumnBuffer::duration_with_capacity(capacity);
		let mut max = ColumnBuffer::duration_with_capacity(capacity);
		let mut p50 = ColumnBuffer::duration_with_capacity(capacity);
		let mut p60 = ColumnBuffer::duration_with_capacity(capacity);
		let mut p70 = ColumnBuffer::duration_with_capacity(capacity);
		let mut p75 = ColumnBuffer::duration_with_capacity(capacity);
		let mut p80 = ColumnBuffer::duration_with_capacity(capacity);
		let mut p85 = ColumnBuffer::duration_with_capacity(capacity);
		let mut p90 = ColumnBuffer::duration_with_capacity(capacity);
		let mut p95 = ColumnBuffer::duration_with_capacity(capacity);
		let mut p98 = ColumnBuffer::duration_with_capacity(capacity);
		let mut p99 = ColumnBuffer::duration_with_capacity(capacity);
		let mut extra_0 = ColumnBuffer::uint8_with_capacity(capacity);
		let mut extra_1 = ColumnBuffer::uint8_with_capacity(capacity);
		let mut extra_2 = ColumnBuffer::uint8_with_capacity(capacity);
		let mut extra_3 = ColumnBuffer::uint8_with_capacity(capacity);

		for record in &records {
			category.push(record.category.name());
			span_names.push(record.span_name.as_str());
			dim_1.push(record.dimensions.first().map(|s| s.as_str()).unwrap_or(""));
			dim_2.push(record.dimensions.get(1).map(|s| s.as_str()).unwrap_or(""));
			calls.push(record.calls);
			total.push(record.total());
			min.push(record.min());
			max.push(record.max());
			let p = record.percentiles();
			p50.push(p.p50);
			p60.push(p.p60);
			p70.push(p.p70);
			p75.push(p.p75);
			p80.push(p.p80);
			p85.push(p.p85);
			p90.push(p.p90);
			p95.push(p.p95);
			p98.push(p.p98);
			p99.push(p.p99);
			let extras = record.extras();
			extra_0.push(extras[0]);
			extra_1.push(extras[1]);
			extra_2.push(extras[2]);
			extra_3.push(extras[3]);
		}

		Columns::new(vec![
			ColumnWithName::new(Fragment::internal("category"), category),
			ColumnWithName::new(Fragment::internal("span_name"), span_names),
			ColumnWithName::new(Fragment::internal("dim_1"), dim_1),
			ColumnWithName::new(Fragment::internal("dim_2"), dim_2),
			ColumnWithName::new(Fragment::internal("calls"), calls),
			ColumnWithName::new(Fragment::internal("total"), total),
			ColumnWithName::new(Fragment::internal("min"), min),
			ColumnWithName::new(Fragment::internal("max"), max),
			ColumnWithName::new(Fragment::internal("p50"), p50),
			ColumnWithName::new(Fragment::internal("p60"), p60),
			ColumnWithName::new(Fragment::internal("p70"), p70),
			ColumnWithName::new(Fragment::internal("p75"), p75),
			ColumnWithName::new(Fragment::internal("p80"), p80),
			ColumnWithName::new(Fragment::internal("p85"), p85),
			ColumnWithName::new(Fragment::internal("p90"), p90),
			ColumnWithName::new(Fragment::internal("p95"), p95),
			ColumnWithName::new(Fragment::internal("p98"), p98),
			ColumnWithName::new(Fragment::internal("p99"), p99),
			ColumnWithName::new(Fragment::internal("extra_0"), extra_0),
			ColumnWithName::new(Fragment::internal("extra_1"), extra_1),
			ColumnWithName::new(Fragment::internal("extra_2"), extra_2),
			ColumnWithName::new(Fragment::internal("extra_3"), extra_3),
		])
	}
}
