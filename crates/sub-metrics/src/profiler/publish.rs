// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::cmp::Reverse;

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_profiler::record::AggregateRecord;
use reifydb_value::{
	fragment::Fragment,
	value::{datetime::DateTime, duration::Duration},
};

pub fn spans_columns(records: &mut Vec<AggregateRecord>, now: DateTime) -> Columns {
	records.sort_by(|a, b| {
		(Reverse(a.total_us), a.category.name(), &a.span_name, &a.dimensions).cmp(&(
			Reverse(b.total_us),
			b.category.name(),
			&b.span_name,
			&b.dimensions,
		))
	});
	let capacity = records.len();

	let mut ts = ColumnBuffer::datetime_with_capacity(capacity);
	let mut category = ColumnBuffer::utf8_with_capacity(capacity);
	let mut span_name = ColumnBuffer::utf8_with_capacity(capacity);
	let mut dim_1 = ColumnBuffer::utf8_with_capacity(capacity);
	let mut dim_2 = ColumnBuffer::utf8_with_capacity(capacity);
	let mut calls = ColumnBuffer::uint8_with_capacity(capacity);
	let mut total = ColumnBuffer::duration_with_capacity(capacity);
	let mut min = ColumnBuffer::duration_with_capacity(capacity);
	let mut p50 = ColumnBuffer::duration_with_capacity(capacity);
	let mut p75 = ColumnBuffer::duration_with_capacity(capacity);
	let mut p90 = ColumnBuffer::duration_with_capacity(capacity);
	let mut p95 = ColumnBuffer::duration_with_capacity(capacity);
	let mut p98 = ColumnBuffer::duration_with_capacity(capacity);
	let mut p99 = ColumnBuffer::duration_with_capacity(capacity);
	let mut max = ColumnBuffer::duration_with_capacity(capacity);
	let mut input_rows = ColumnBuffer::uint8_with_capacity(capacity);
	let mut output_rows = ColumnBuffer::uint8_with_capacity(capacity);
	let mut lock_wait = ColumnBuffer::duration_with_capacity(capacity);
	let mut store_reads = ColumnBuffer::uint8_with_capacity(capacity);

	for record in records.iter() {
		ts.push(now);
		category.push(record.category.name());
		span_name.push(record.span_name.as_str());
		dim_1.push(record.dimensions.first().map(|s| s.as_str()).unwrap_or(""));
		dim_2.push(record.dimensions.get(1).map(|s| s.as_str()).unwrap_or(""));
		calls.push(record.calls);
		total.push(record.total());
		min.push(record.min());
		let percentiles = record.percentiles();
		p50.push(percentiles.p50);
		p75.push(percentiles.p75);
		p90.push(percentiles.p90);
		p95.push(percentiles.p95);
		p98.push(percentiles.p98);
		p99.push(percentiles.p99);
		max.push(record.max());
		let extras = record.extras();
		input_rows.push(extras[0]);
		output_rows.push(extras[1]);
		lock_wait
			.push(Duration::from_microseconds(extras[2].min(9_000_000_000_000_000) as i64)
				.unwrap_or_default());
		store_reads.push(extras[3]);
	}

	Columns::new(vec![
		ColumnWithName::new(Fragment::internal("ts"), ts),
		ColumnWithName::new(Fragment::internal("category"), category),
		ColumnWithName::new(Fragment::internal("span_name"), span_name),
		ColumnWithName::new(Fragment::internal("dim_1"), dim_1),
		ColumnWithName::new(Fragment::internal("dim_2"), dim_2),
		ColumnWithName::new(Fragment::internal("calls"), calls),
		ColumnWithName::new(Fragment::internal("total"), total),
		ColumnWithName::new(Fragment::internal("min"), min),
		ColumnWithName::new(Fragment::internal("p50"), p50),
		ColumnWithName::new(Fragment::internal("p75"), p75),
		ColumnWithName::new(Fragment::internal("p90"), p90),
		ColumnWithName::new(Fragment::internal("p95"), p95),
		ColumnWithName::new(Fragment::internal("p98"), p98),
		ColumnWithName::new(Fragment::internal("p99"), p99),
		ColumnWithName::new(Fragment::internal("max"), max),
		ColumnWithName::new(Fragment::internal("input_rows"), input_rows),
		ColumnWithName::new(Fragment::internal("output_rows"), output_rows),
		ColumnWithName::new(Fragment::internal("lock_wait"), lock_wait),
		ColumnWithName::new(Fragment::internal("store_reads"), store_reads),
	])
}
