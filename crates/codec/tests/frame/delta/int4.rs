// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::Int32Array;
use reifydb_value::value::value_type::ValueType;

use crate::common::{ColumnData, data};

fn make(v: Vec<i32>) -> ColumnData {
	data(ValueType::Int4, Int32Array::from(v))
}

crate::delta_tests! {
	ascending: (0..200i32).collect::<Vec<_>>(),
	descending: (0..200i32).rev().collect::<Vec<_>>(),
	unsorted: (0..200).map(|i| (i * 7 + 13) % 97).collect::<Vec<_>>(),
}
