// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::wide_int_array::wide_array, value_type::ValueType};

use crate::common::{ColumnData, data};

fn make(v: Vec<i128>) -> ColumnData {
	data(ValueType::Int16, wide_array(v))
}

crate::plain_tests! {
	typical: vec![-1_000_000_000_000i128, 0, 42, 1_000_000_000_000],
	boundary: vec![i128::MIN, -1, 0, 1, i128::MAX],
	single: 0i128,
}
