// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::UInt8Array;
use reifydb_value::value::value_type::ValueType;

use crate::common::{ColumnData, data};

fn make(v: Vec<u8>) -> ColumnData {
	data(ValueType::Uint1, UInt8Array::from(v))
}

crate::plain_tests! {
	typical: vec![0u8, 1, 128, 255],
	boundary: vec![u8::MIN, 1, u8::MAX],
	single: 0u8,
}
