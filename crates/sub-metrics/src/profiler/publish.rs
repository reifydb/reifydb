// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::cmp::Reverse;

use arrow_array::RecordBatch;
use reifydb_core::value::{batch::batch, column::builder::ColumnBuilder};
use reifydb_profiler::record::AggregateRecord;
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration, value_type::ValueType},
};

pub fn spans_columns(records: &mut [AggregateRecord], now: DateTime) -> Result<RecordBatch> {
	records.sort_by(|a, b| {
		(Reverse(a.total_us), a.category.name(), &a.span_name, &a.dimensions).cmp(&(
			Reverse(b.total_us),
			b.category.name(),
			&b.span_name,
			&b.dimensions,
		))
	});
	let capacity = records.len();

	let mut ts = ColumnBuilder::with_capacity(ValueType::DateTime, capacity);
	let mut category = ColumnBuilder::with_capacity(ValueType::Utf8, capacity);
	let mut span_name = ColumnBuilder::with_capacity(ValueType::Utf8, capacity);
	let mut dim_1 = ColumnBuilder::with_capacity(ValueType::Utf8, capacity);
	let mut dim_2 = ColumnBuilder::with_capacity(ValueType::Utf8, capacity);
	let mut calls = ColumnBuilder::with_capacity(ValueType::Uint8, capacity);
	let mut total = ColumnBuilder::with_capacity(ValueType::Duration, capacity);
	let mut p50 = ColumnBuilder::with_capacity(ValueType::Duration, capacity);
	let mut p75 = ColumnBuilder::with_capacity(ValueType::Duration, capacity);
	let mut p90 = ColumnBuilder::with_capacity(ValueType::Duration, capacity);
	let mut p95 = ColumnBuilder::with_capacity(ValueType::Duration, capacity);
	let mut p98 = ColumnBuilder::with_capacity(ValueType::Duration, capacity);
	let mut p99 = ColumnBuilder::with_capacity(ValueType::Duration, capacity);
	let mut p100 = ColumnBuilder::with_capacity(ValueType::Duration, capacity);
	let mut input_rows = ColumnBuilder::with_capacity(ValueType::Uint8, capacity);
	let mut output_rows = ColumnBuilder::with_capacity(ValueType::Uint8, capacity);
	let mut lock_wait = ColumnBuilder::with_capacity(ValueType::Duration, capacity);

	for record in records.iter() {
		ts.push(now);
		category.push(record.category.name());
		span_name.push(record.span_name.as_str());
		dim_1.push(record.dimensions.first().map(|s| s.as_str()).unwrap_or(""));
		dim_2.push(record.dimensions.get(1).map(|s| s.as_str()).unwrap_or(""));
		calls.push(record.calls);
		total.push(record.total());
		let percentiles = record.percentiles();
		p50.push(percentiles.p50);
		p75.push(percentiles.p75);
		p90.push(percentiles.p90);
		p95.push(percentiles.p95);
		p98.push(percentiles.p98);
		p99.push(percentiles.p99);
		p100.push(percentiles.p100);
		let extras = record.extras();
		input_rows.push(extras[0]);
		output_rows.push(extras[1]);
		lock_wait.push(Duration::from_micros_infallible(extras[2]));
	}

	batch(vec![
		ts.finish("ts"),
		category.finish("category"),
		span_name.finish("span_name"),
		dim_1.finish("dim_1"),
		dim_2.finish("dim_2"),
		calls.finish("calls"),
		total.finish("total"),
		p50.finish("p50"),
		p75.finish("p75"),
		p90.finish("p90"),
		p95.finish("p95"),
		p98.finish("p98"),
		p99.finish("p99"),
		p100.finish("p100"),
		input_rows.finish("input_rows"),
		output_rows.finish("output_rows"),
		lock_wait.finish("lock_wait"),
	])
}
