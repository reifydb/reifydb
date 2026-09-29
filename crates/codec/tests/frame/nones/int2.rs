// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::Int16Array;
use reifydb_value::value::value_type::ValueType;

use crate::common::{ColumnData, data};

fn make(v: Vec<i16>) -> ColumnData {
	data(ValueType::Int2, Int16Array::from(v))
}

crate::nones_tests! {
	values: vec![-1000i16, 0, 42, 12345, i16::MAX],
	inner_type: ValueType::Int2,
}
