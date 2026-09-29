// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::temporal_array::duration_array, duration::Duration, value_type::ValueType};

use crate::common::{ColumnData, data};

fn make(v: Vec<Duration>) -> ColumnData {
	data(ValueType::Duration, duration_array(v))
}

crate::plain_tests! {
	typical: vec![
		Duration::new(0, 0, 0).unwrap(),
		Duration::new(1, 2, 3_000_000_000).unwrap(),
		Duration::new(-1, -2, -3_000_000_000).unwrap(),
	],
	boundary: vec![
		Duration::new(0, 0, 0).unwrap(),
		Duration::new(12, 365, 86_400_000_000_000).unwrap(),
		Duration::new(-12, -365, -86_400_000_000_000).unwrap(),
	],
	single: Duration::new(0, 0, 0).unwrap(),
}
