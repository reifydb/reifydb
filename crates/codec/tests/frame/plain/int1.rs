// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::Int8Array;
use reifydb_value::value::value_type::ValueType;

use crate::common::{ColumnData, data};

fn make(v: Vec<i8>) -> ColumnData {
	data(ValueType::Int1, Int8Array::from(v))
}

crate::plain_tests! {
	typical: vec![-10i8, 0, 42, 100],
	boundary: vec![i8::MIN, -1, 0, 1, i8::MAX],
	single: 0i8,
}
