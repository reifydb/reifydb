// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::Int64Array;
use reifydb_value::value::value_type::ValueType;

use crate::common::{ColumnData, data};

fn make(v: Vec<i64>) -> ColumnData {
	data(ValueType::Int8, Int64Array::from(v))
}

crate::delta_rle_tests! {
	constant_stride: (1..=500i64).collect::<Vec<_>>(),
	descending_stride: (1..=500i64).rev().collect::<Vec<_>>(),
}
