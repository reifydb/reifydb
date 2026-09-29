// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::wide_int_array::wide_array, value_type::ValueType};

use crate::common::{ColumnData, data};

fn make(v: Vec<i128>) -> ColumnData {
	data(ValueType::Int16, wide_array(v))
}

crate::delta_tests! {
	ascending: (0..200i128).collect::<Vec<_>>(),
	descending: (0..200i128).rev().collect::<Vec<_>>(),
	unsorted: (0..200).map(|i| ((i * 7 + 13) % 97) as i128).collect::<Vec<_>>(),
}
