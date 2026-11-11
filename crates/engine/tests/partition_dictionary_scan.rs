// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{params::Params, value::Value};

const ROWS: &str = "[{ id: 1, pool: 'a' }, { id: 2, pool: 'b' }, { id: 3, pool: 'a' }, { id: 4, pool: 'c' }]";

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE DICTIONARY test::pools FOR utf8 AS uint4");
	t.admin(
		"CREATE TABLE test::t { id: int4, pool: utf8 with { dictionary: test::pools } } WITH { partition: { by: { pool } } }",
	);
	t.admin(
		"CREATE TRANSACTIONAL VIEW test::v { id: int4, pool: utf8 with { dictionary: test::pools } } WITH { partition: { by: { pool } } } AS { FROM test::t }",
	);
	t.admin(
		"CREATE RINGBUFFER test::rb { id: int4, pool: utf8 with { dictionary: test::pools } } WITH { capacity: 10, partition: { by: { pool } } }",
	);
	t.admin(
		"CREATE RINGBUFFER test::plain { id: int4, pool: utf8 } WITH { capacity: 10, partition: { by: { pool } } }",
	);
	for source in ["test::t", "test::rb", "test::plain"] {
		t.command(&format!("INSERT {source} {ROWS}"));
	}
	t
}

#[test]
fn a_dictionary_partition_key_answers_exactly_what_the_plain_filter_answers() {
	// The wrapped filter never prunes, so any difference means the id lookup pruned to the wrong partition,
	// or a miss returned a different shape than a scan that found nothing.
	let t = engine();
	let keys = [
		Value::Utf8("a".to_string()),
		Value::Utf8("b".to_string()),
		Value::Utf8("never".to_string()),
		Value::Int4(3),
		Value::none(),
	];
	for source in ["test::t", "test::v", "test::rb", "test::plain"] {
		for key in &keys {
			let pruned = run(
				&t,
				&format!(
					"FROM {source} | filter {{ pool == $1 }} | sort {{ id: ASC }} | map {{ v: id }}"
				),
				positional(vec![key.clone()]),
			);
			let plain = run(
				&t,
				&format!(
					"FROM {source} | filter {{ (pool == $1) == true }} | sort {{ id: ASC }} | map {{ v: id }}"
				),
				positional(vec![key.clone()]),
			);
			assert_eq!(pruned, plain, "{source} with {key:?}");
		}
	}
}

#[test]
fn a_dictionary_partition_key_answers_the_rows_of_its_partition() {
	// A pruned scan must read every row stored under the value's id; hashing the text would read none.
	let t = engine();
	for source in ["test::t", "test::v", "test::rb", "test::plain"] {
		let by_key = format!("FROM {source} | filter {{ pool == $1 }} | sort {{ id: ASC }} | map {{ v: id }}");
		assert_eq!(
			run(&t, &by_key, positional(vec![Value::Utf8("a".to_string())])),
			ids(&["1", "3"]),
			"{source}"
		);
		assert_eq!(run(&t, &by_key, positional(vec![Value::Utf8("never".to_string())])), ids(&[]), "{source}");

		let constant =
			format!("FROM {source} | filter {{ pool == 'b' }} | sort {{ id: ASC }} | map {{ v: id }}");
		assert_eq!(run(&t, &constant, Params::None), ids(&["2"]), "{source}");

		let listed = format!(
			"FROM {source} | filter {{ pool in ['a', 'never', 'c'] }} | sort {{ id: ASC }} | map {{ v: id }}"
		);
		assert_eq!(run(&t, &listed, Params::None), ids(&["1", "3", "4"]), "{source}");

		let none_listed = format!(
			"FROM {source} | filter {{ pool in ['never'] }} | sort {{ id: ASC }} | map {{ v: id }}"
		);
		assert_eq!(run(&t, &none_listed, Params::None), ids(&[]), "{source}");
	}
}

#[test]
fn a_partition_key_on_a_ringbuffer_plans_the_run_time_partition_scan() {
	// Without the run-time node every scan reads every partition and still answers the right rows, so only
	// the plan shows the pruning is gone.
	let t = engine();
	for (source, name) in [("test::rb", "rb"), ("test::plain", "plain")] {
		let plan = explain(&t, &format!("from {source} filter {{pool == $1}}"));
		assert_eq!(
			plan.iter().map(|(kind, _)| kind.as_str()).collect::<Vec<_>>(),
			["Filter", "PartitionParameterScan", "RingBufferScan"],
			"{source}"
		);
		let key = if name == "rb" {
			"dictionary($1)"
		} else {
			"$1"
		};
		assert_eq!(plan[1].1, format!("source={name} keys=[pool={key}]"), "{source}");
	}

	let constant = explain(&t, "from test::rb filter {pool == 'a'}");
	assert_eq!(
		constant[1],
		("PartitionParameterScan".to_string(), "source=rb keys=[pool=dictionary(a)]".to_string())
	);

	let table = explain(&t, "from test::t filter {pool == $1}");
	assert_eq!(table[1].1, "source=t keys=[pool=dictionary($1)]");
}

fn explain(t: &TestEngine, rql: &str) -> Vec<(String, String)> {
	let result = t.inner().query_as(TestEngine::identity(), &format!("CALL rql::explain(\"{rql}\")"), Params::None);
	assert!(result.error.is_none(), "{:?}", result.error);
	let frame = result.frames.last().expect("a frame");
	let kind = frame.column("kind").unwrap().expect("column kind");
	let detail = frame.column("detail").unwrap().expect("column detail");
	(0..kind.len()).map(|i| (kind.get_value(i).to_string(), detail.get_value(i).to_string())).collect()
}

#[derive(Debug, PartialEq)]
enum Answer {
	Rows(Vec<String>),
	Error(String, String),
}

fn positional(values: Vec<Value>) -> Params {
	Params::Positional(Arc::new(values))
}

fn ids(values: &[&str]) -> Answer {
	Answer::Rows(values.iter().map(|v| v.to_string()).collect())
}

fn run(t: &TestEngine, rql: &str, params: Params) -> Answer {
	let result = t.inner().query_as(TestEngine::identity(), rql, params);
	if let Some(err) = result.error {
		let diagnostic = err.diagnostic();
		return Answer::Error(diagnostic.code.clone(), diagnostic.message.clone());
	}
	let column = result.frames.last().expect("a frame").column("v").unwrap().expect("column v");
	Answer::Rows((0..column.len()).map(|i| column.get_value(i).to_string()).collect())
}
