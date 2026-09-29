// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::temporal_array::time_array, time::Time, value_type::ValueType};

use crate::common::{ColumnData, data};

fn make(v: Vec<Time>) -> ColumnData {
	data(ValueType::Time, time_array(v))
}

crate::delta_tests! {
	ascending: (0..500u64).map(|i| Time::from_nanos_since_midnight(i * 100_000_000).unwrap()).collect::<Vec<_>>(),
	descending: (0..500u64).rev().map(|i| Time::from_nanos_since_midnight(i * 100_000_000).unwrap()).collect::<Vec<_>>(),
	unsorted: (0..500u64).map(|i| Time::from_nanos_since_midnight(((i * 7 + 13) % 500) * 100_000_000).unwrap()).collect::<Vec<_>>(),
}
