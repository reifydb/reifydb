// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::Int64Array;
use reifydb_value::value::frame::data::FrameColumnData;

fn make(v: Vec<i64>) -> FrameColumnData {
	FrameColumnData::Int8(Int64Array::from(v))
}

crate::plain_tests! {
	typical: vec![-1_000_000_000i64, 0, 42, 1_000_000_000],
	boundary: vec![i64::MIN, -1, 0, 1, i64::MAX],
	single: 0i64,
}
