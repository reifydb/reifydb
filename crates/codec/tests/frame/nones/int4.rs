// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::Int32Array;
use reifydb_value::value::value_type::ValueType;

use crate::common::{ColumnData, data};

fn make(v: Vec<i32>) -> ColumnData {
	data(ValueType::Int4, Int32Array::from(v))
}

crate::nones_tests! {
	values: vec![-2_000_000i32, 0, 42, 2_000_000, i32::MAX],
	inner_type: ValueType::Int4,
}
