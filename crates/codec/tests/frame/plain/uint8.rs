// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::UInt64Array;
use reifydb_value::value::value_type::ValueType;

use crate::common::{ColumnData, data};

fn make(v: Vec<u64>) -> ColumnData {
	data(ValueType::Uint8, UInt64Array::from(v))
}

crate::plain_tests! {
	typical: vec![0u64, 1, 1_000_000_000, u64::MAX - 1],
	boundary: vec![u64::MIN, 1, u64::MAX],
	single: 0u64,
}
