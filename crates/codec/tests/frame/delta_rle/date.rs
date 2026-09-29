// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::temporal_array::date_array, date::Date, value_type::ValueType};

use crate::common::{ColumnData, data};

fn make(v: Vec<Date>) -> ColumnData {
	data(ValueType::Date, date_array(v))
}

crate::delta_rle_tests! {
	constant_stride: (0..500).map(|i| Date::from_days_since_epoch(18000 + i).unwrap()).collect::<Vec<_>>(),
	descending_stride: (0..500).rev().map(|i| Date::from_days_since_epoch(18000 + i).unwrap()).collect::<Vec<_>>(),
}
