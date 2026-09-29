// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::BooleanArray;
use reifydb_value::value::value_type::ValueType;

use crate::common::{ColumnData, data};

fn make(v: Vec<bool>) -> ColumnData {
	data(ValueType::Boolean, BooleanArray::from(v))
}

crate::nones_tests! {
	values: vec![true, false, true, true, false],
	inner_type: ValueType::Boolean,
}
