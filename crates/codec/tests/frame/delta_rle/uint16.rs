// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::wide_int_array::wide_array, value_type::ValueType};

use crate::common::{ColumnData, data};

fn make(v: Vec<u128>) -> ColumnData {
	data(ValueType::Uint16, wide_array(v))
}

crate::delta_rle_tests! {
	constant_stride: (1..=500u128).collect::<Vec<_>>(),
	descending_stride: (1..=500u128).rev().collect::<Vec<_>>(),
}
