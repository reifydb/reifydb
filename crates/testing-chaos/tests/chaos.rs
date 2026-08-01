// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Self-tests for the chaos substrate. They live in an integration target so `chaos_test!`
//! resolves through the real external crate path; an in-crate unit test would resolve it via
//! `extern crate self` and could not catch a wrong path in the generated code.

use reifydb_testing_chaos::{
	corpus::{Corpus, mix},
	fuzz::{pick, split},
};
use reifydb_testing_macro::chaos_test;

chaos_test!(the_macro_expands_and_threads_its_seed, 3, |seed| {
	// A wrong generated path fails to compile; the arithmetic proves the seed reached the body.
	assert_eq!(seed.wrapping_mul(2), seed.wrapping_add(seed));
});

#[test]
#[ignore = "spawned as a subprocess by the_seed_environment_variable_is_still_named_seed"]
fn seed_probe_that_always_fails() {
	// Not a real test: it exists so the probe below can drive run_iteration's real failure path
	// in a child process and read back the seed it resolved.
	reifydb_testing_chaos::seed::run_iteration("probe", 0, |_| panic!("probe"));
}

#[test]
fn the_seed_environment_variable_is_still_named_seed() {
	// Nothing else asserts the NAME of the environment variable run_iteration reads: typo it and
	// every chaos test still passes while SEED silently stops pinning anything. Driving the real
	// path through a child process is the only way to observe the name.
	let exe = std::env::current_exe().expect("the running test binary has a path");
	let output = std::process::Command::new(exe)
		.args(["--exact", "seed_probe_that_always_fails", "--ignored", "--nocapture"])
		.env("SEED", "777")
		.output()
		.expect("re-running this test binary as a child must succeed");

	let report = String::from_utf8_lossy(&output.stderr);
	assert!(
		report.contains("seed:      777"),
		"a pinned SEED must reach the failure report, but it read:\n{report}"
	);
	assert!(
		report.contains("FILTER=probe_0"),
		"the report must name the test so the printed replay command actually targets it:\n{report}"
	);
}

#[test]
fn a_corpus_accepts_its_own_fingerprint_and_rejects_any_other() {
	// assert_pinned turns a silently re-pointed regression into a loud failure, so it has to be
	// exact: the recorded value passes and a value one bit away does not.
	let corpus = Corpus::new(0xDEAD_BEEF, 12);
	corpus.assert_pinned(0xDEAD_BEEF);

	let outcome = std::panic::catch_unwind(|| Corpus::new(0xDEAD_BEEF, 12).assert_pinned(0xDEAD_BEEE));
	assert!(outcome.is_err(), "a fingerprint that does not match its pin must fail, or stale pins go unnoticed");
}

#[test]
fn mixing_is_order_sensitive_and_stable() {
	// Pinned fingerprints make this arithmetic a compatibility surface. Order sensitivity is what
	// makes a fingerprint describe a sequence rather than a multiset of operations.
	assert_eq!(mix(0, 1), mix(0, 1), "mixing must be deterministic");
	assert_ne!(mix(mix(0, 1), 2), mix(mix(0, 2), 1), "swapping two operations must change the fingerprint");
	assert_ne!(mix(0, 1), mix(1, 0), "state and value must not be interchangeable");
}

#[test]
fn splitting_decorrelates_the_parameter_stream_from_the_corpus_stream() {
	// Parameters decide what is under test and the sequence seed decides the corpus. Sharing a
	// stream would let a widened parameter range reshuffle every corpus.
	let (mut params_a, sequence_a) = split(7);
	let (mut params_b, sequence_b) = split(8);
	assert_ne!(sequence_a, sequence_b, "different master seeds must give different corpora");

	const OPTIONS: [u64; 4] = [10, 20, 30, 40];
	let drawn_a: Vec<u64> = (0..16).map(|_| pick(&mut params_a, &OPTIONS)).collect();
	let drawn_b: Vec<u64> = (0..16).map(|_| pick(&mut params_b, &OPTIONS)).collect();
	assert_ne!(drawn_a, drawn_b, "different master seeds must give different parameter draws");
	assert!(drawn_a.iter().all(|d| OPTIONS.contains(d)), "pick must only ever return one of its options");

	let (_, sequence_again) = split(7);
	assert_eq!(sequence_a, sequence_again, "the same master seed must replay the same sequence seed");
}

#[test]
fn tolerant_containment_accepts_float_drift_and_still_rejects_a_wrong_value() {
	// One comparator serves both sides: the host compares integers exactly, the guest needs
	// per-column latitude for a different summation order. A tolerance of None must stay bit
	// equality, not "close enough".
	use reifydb_testing_chaos::operator::compare::contains_all;
	use reifydb_value::value::Value;

	let actual = vec![vec![Value::Int4(1), Value::float8(10.000_000_1_f64)]];
	let wanted = vec![vec![Value::Int4(1), Value::float8(10.0_f64)]];

	assert!(!contains_all(&actual, &wanted, &[]), "with no tolerance the drift must be a mismatch");
	assert!(
		contains_all(&actual, &wanted, &[None, Some(1e-6)]),
		"a tolerance on the float column must absorb drift below it"
	);
	assert!(
		!contains_all(&actual, &wanted, &[None, Some(1e-9)]),
		"a tolerance tighter than the drift must still reject"
	);

	let wrong_group = vec![vec![Value::Int4(2), Value::float8(10.0_f64)]];
	assert!(
		!contains_all(&wrong_group, &wanted, &[None, Some(1.0)]),
		"a tolerance on one column must not excuse a mismatch in another"
	);
}
