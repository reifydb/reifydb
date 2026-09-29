// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::temporal_array::datetime_array, datetime::DateTime, value_type::ValueType};

use crate::common::{ColumnData, data};

fn make(v: Vec<DateTime>) -> ColumnData {
	data(ValueType::DateTime, datetime_array(v))
}

crate::plain_tests! {
	typical: vec![
		DateTime::from_nanos(0),
		DateTime::from_nanos(1_700_000_000_000_000_000),
		DateTime::from_nanos(1_000_000),
	],
	boundary: vec![
		DateTime::from_nanos(0),
		DateTime::from_nanos(i64::MAX),
	],
	single: DateTime::from_nanos(0),
}
