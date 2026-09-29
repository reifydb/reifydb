// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_value::value::{Value, column_view::FromColumnView, system_columns::column_view};

#[path = "sdk/row_time.rs"]
mod row_time;

#[path = "sdk/writer.rs"]
mod writer;

#[path = "sdk/batch.rs"]
mod batch;

#[path = "sdk/tumbling.rs"]
mod tumbling;

#[path = "sdk/rolling.rs"]
mod rolling;

#[path = "sdk/rolling_top_k.rs"]
mod rolling_top_k;

#[path = "sdk/tumbling_carry.rs"]
mod tumbling_carry;

#[path = "sdk/carry.rs"]
mod carry;

#[path = "sdk/plain_rolling.rs"]
mod plain_rolling;

#[path = "sdk/plain_top_k.rs"]
mod plain_top_k;

#[path = "sdk/retained.rs"]
mod retained;

#[path = "sdk/guest_sweep.rs"]
mod guest_sweep;

#[path = "sdk/class.rs"]
mod class;

#[path = "sdk/plain_sliding.rs"]
mod plain_sliding;

#[path = "sdk/plain_session.rs"]
mod plain_session;

#[path = "sdk/windowed_read_error.rs"]
mod windowed_read_error;

fn read<T: FromColumnView>((batch, row): (&RecordBatch, usize), name: &str) -> Option<T> {
	column_view(batch, name).unwrap().and_then(|column| column.get_as::<T>(row).unwrap())
}

fn read_value((batch, row): (&RecordBatch, usize), name: &str) -> Option<Value> {
	column_view(batch, name).unwrap().map(|column| column.get_value(row))
}
