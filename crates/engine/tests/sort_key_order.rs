// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, str::FromStr};

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	params::Params,
	value::{Value, decimal::Decimal, duration::Duration, identity::IdentityId, time::Time},
};

fn ids(rql: &str, params: Params) -> Vec<String> {
	// Row order is the whole point of these pins, so read the id column back in the order the sort emitted it.
	let t = TestEngine::new();
	let r = t.inner().query_as(TestEngine::identity(), rql, params);
	if let Some(e) = r.error {
		panic!("query failed: {e:?}\nrql: {rql}")
	}
	assert_eq!(r.frames.len(), 1, "expected one frame, got {}", r.frames.len());
	let column = r.frames[0].columns.iter().find(|c| c.name == "id").expect("column id");
	(0..column.data.len()).map(|row| column.data.get_value(row).to_string()).collect()
}

fn sorted_ids(values: Vec<Value>, tail: &str) -> Vec<String> {
	// Each value rides in as a param so the test pins the value type itself, not a literal's parse.
	let mut params = HashMap::new();
	let mut rows = Vec::new();
	for (index, value) in values.into_iter().enumerate() {
		params.insert(format!("v{index}"), value);
		rows.push(format!("{{ id: {index}, v: $v{index} }}"));
	}
	let rql = format!("from [{}] | {tail}", rows.join(", "));
	ids(&rql, Params::from(params))
}

#[test]
fn sort_none_placement_by_direction() {
	// A none sorts above every value, so ascending must end on it and descending must open on it.
	let rows = "from [{ id: 0, v: 2 }, { id: 1, v: none }, { id: 2, v: 1 }]";

	let ascending = ids(&format!("{rows} | sort {{ v: ASC }}"), Params::None);
	let descending = ids(&format!("{rows} | sort {{ v: DESC }}"), Params::None);

	assert_eq!(ascending, vec!["2", "0", "1"]);
	assert_eq!(descending, vec!["1", "0", "2"]);
}

#[test]
fn sort_float_total_order() {
	// Floats are ordered by the total order of their values, so the raw sign bit must not lift negatives to the
	// top.
	let values = vec![Value::float8(2.5), Value::float8(-1.5), Value::float8(0.0), Value::float8(-0.25)];

	assert_eq!(sorted_ids(values, "sort { v: ASC }"), vec!["1", "3", "2", "0"]);
}

#[test]
fn sort_duration_months_before_days() {
	// A month is not thirty days: months decide first, so a 31 day span stays below a one month span.
	let month = Duration::new(1, 0, 0).expect("a one month duration");
	let days = Duration::new(0, 31, 0).expect("a thirty one day duration");
	let values = vec![Value::Duration(month), Value::Duration(days)];

	assert_eq!(sorted_ids(values, "sort { v: ASC }"), vec!["1", "0"]);
}

#[test]
fn sort_decimal_equal_values_across_scales() {
	// Decimals compare numerically, so 1.0 and 1.00 tie and must keep the order they arrived in.
	let decimal = |text: &str| Value::Decimal(Decimal::from_str(text).expect("a decimal literal"));
	let values = vec![decimal("1.0"), decimal("1.00"), decimal("0.5")];

	assert_eq!(sorted_ids(values, "sort { v: ASC }"), vec!["2", "0", "1"]);
}

#[test]
fn sort_uuid7_byte_order() {
	// These three differ only deep in their leading bytes, so any little-endian read flips system above root.
	let values = vec![
		Value::Uuid7(IdentityId::root().value()),
		Value::Uuid7(IdentityId::anonymous().value()),
		Value::Uuid7(IdentityId::system().value()),
	];

	assert_eq!(sorted_ids(values, "sort { v: ASC }"), vec!["1", "2", "0"]);
}

#[test]
fn sort_time_at_the_end_of_the_day() {
	// The last nanosecond of the day must stay the largest time rather than wrapping below midnight.
	let time = |hour: u32, minute: u32, second: u32, nano: u32| {
		Value::Time(Time::new(hour, minute, second, nano).expect("a time of day"))
	};
	let values = vec![time(23, 59, 59, 999_999_999), time(0, 0, 0, 0), time(12, 0, 0, 0)];

	assert_eq!(sorted_ids(values, "sort { v: ASC }"), vec!["1", "2", "0"]);
}

#[test]
fn sort_ties_keep_input_order() {
	// Rows tied on the key must keep their input position in both directions, or take turns non-deterministic.
	let rows = "from [{ id: 0, v: 7 }, { id: 1, v: 7 }, { id: 2, v: 7 }, { id: 3, v: 7 }]";

	let ascending = ids(&format!("{rows} | sort {{ v: ASC }}"), Params::None);
	let descending = ids(&format!("{rows} | sort {{ v: DESC }}"), Params::None);

	assert_eq!(ascending, vec!["0", "1", "2", "3"]);
	assert_eq!(descending, vec!["0", "1", "2", "3"]);
}

#[test]
fn top_k_ties_keep_the_earliest_rows() {
	// A later tied row must never displace an earlier one that the top-k node is already holding.
	let rows = "from [{ id: 0, v: 5 }, { id: 1, v: 5 }, { id: 2, v: 5 }, { id: 3, v: 5 }, { id: 4, v: 5 }]";

	let kept = ids(&format!("{rows} | sort {{ v: ASC }} | take 2"), Params::None);

	assert_eq!(kept, vec!["0", "1"]);
}
