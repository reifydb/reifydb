// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::iter::repeat_n;

use reifydb_value::value::{container::temporal_array::date_array, date::Date, value_type::ValueType};

use crate::common::{ColumnData, data};

fn make(v: Vec<Date>) -> ColumnData {
	data(ValueType::Date, date_array(v))
}

crate::rle_tests! {
	repeated: {
		let mut v = Vec::new();
		for days in [0i32, 1000, 2000, 3000, 4000] {
			let d = Date::from_days_since_epoch(days).unwrap();
			v.extend(repeat_n(d, 100));
		}
		v
	},
	unique: (0..100).map(|i| Date::from_days_since_epoch(i * 7).unwrap()).collect::<Vec<_>>(),
}
