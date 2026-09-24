// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, str::FromStr};

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	params::Params,
	value::{Value, decimal::Decimal, frame::frame::Frame},
};

fn query(t: &TestEngine, rql: &str, params: Params) -> Vec<Frame> {
	let r = t.inner().query_as(TestEngine::identity(), rql, params);
	if let Some(e) = r.error {
		panic!("query failed: {e:?}\nrql: {rql}")
	}
	r.frames
}

fn zeros() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::f { id: int4, z: float8, sign: float8 }");
	t.admin("CREATE TABLE test::g { k: float8 }");
	t.command("INSERT test::f [{ id: 1, z: 0.0, sign: 1.0 }, { id: 2, z: 0.0, sign: -1.0 }]");
	t.command("INSERT test::g [{ k: 0.0 }]");
	t
}

fn decimals() -> Params {
	Params::from(HashMap::from([
		("a".to_string(), Value::Decimal(Decimal::from_str("1.0").unwrap())),
		("b".to_string(), Value::Decimal(Decimal::from_str("1.00").unwrap())),
	]))
}

#[test]
fn hash_join_matches_negative_zero_with_zero() {
	// Distinct and group by already treat 0.0 and -0.0 as one key, so a join must not split them.
	let t = zeros();

	let frames = query(
		&t,
		"FROM test::f map { id, k: z * sign } INNER JOIN { FROM test::g } AS s USING (k, s.k)",
		Params::None,
	);

	assert_eq!(TestEngine::row_count(&frames), 2, "both 0.0 and -0.0 must match 0.0, got:\n{}", frames[0]);
}

#[test]
fn natural_join_matches_negative_zero_with_zero() {
	// The natural join builds its own key map, so it needs the same float rule as the hash join.
	let t = zeros();

	let frames = query(&t, "FROM test::f map { id, k: z * sign } NATURAL JOIN { FROM test::g } AS s", Params::None);

	assert_eq!(TestEngine::row_count(&frames), 2, "both 0.0 and -0.0 must match 0.0, got:\n{}", frames[0]);
}

#[test]
fn hash_join_matches_decimals_that_differ_only_in_scale() {
	// 1.0 and 1.00 are the same number, so a byte compare on the scale would drop a real match.
	let t = TestEngine::new();

	let frames = query(&t, "from [{ k: $a }] INNER JOIN { from [{ k: $b }] } AS s USING (k, s.k)", decimals());

	assert_eq!(TestEngine::row_count(&frames), 1, "1.0 must match 1.00, got:\n{}", frames[0]);
}

#[test]
fn natural_join_matches_decimals_that_differ_only_in_scale() {
	// The natural join builds its own key map, so it needs the same decimal rule as the hash join.
	let t = TestEngine::new();

	let frames = query(&t, "from [{ k: $a }] NATURAL JOIN { from [{ k: $b }] } AS s", decimals());

	assert_eq!(TestEngine::row_count(&frames), 1, "1.0 must match 1.00, got:\n{}", frames[0]);
}

#[test]
fn a_none_key_never_matches_a_none_key() {
	// The row key encodes a none as a value, so without the skip two none keys would pair up.
	let t = TestEngine::new();
	let rql = "from [{ id: 1, k: none }, { id: 2, k: 1 }] INNER JOIN { from [{ k: none }, { k: 1 }] } AS s USING (k, s.k)";

	let frames = query(&t, rql, Params::None);

	assert_eq!(TestEngine::row_count(&frames), 1, "only k = 1 may match, got:\n{}", frames[0]);
}

#[test]
fn a_left_join_keeps_a_none_key_row_unmatched() {
	// A left join must still return the none key row, just without a partner.
	let t = TestEngine::new();
	let rql = "from [{ id: 1, k: none }, { id: 2, k: 1 }] LEFT JOIN { from [{ k: none }, { k: 1 }] } AS s USING (k, s.k)";

	let frames = query(&t, rql, Params::None);

	assert_eq!(TestEngine::row_count(&frames), 2, "the none key row must survive unmatched, got:\n{}", frames[0]);
}

#[test]
fn a_natural_join_never_matches_none_keys() {
	// The natural join skips none keys on its own path, so it needs its own pin.
	let t = TestEngine::new();
	let rql = "from [{ id: 1, k: none }, { id: 2, k: 1 }] NATURAL JOIN { from [{ k: none }, { k: 1 }] } AS s";

	let frames = query(&t, rql, Params::None);

	assert_eq!(TestEngine::row_count(&frames), 1, "only k = 1 may match, got:\n{}", frames[0]);
}
