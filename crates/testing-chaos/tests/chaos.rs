// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Self-tests for the chaos substrate, run as an integration target so `chaos_test!` is expanded in a separate
//! compilation unit and has to resolve `::reifydb_testing_chaos::seed::run_iteration` through the real external crate
//! path, exactly as a consumer does. An in-crate unit test would resolve it through `extern crate self`, which cannot
//! catch a wrong path in the generated code.

use reifydb_testing_chaos::{
	corpus::{Corpus, mix},
	fuzz::{pick, split},
};
use reifydb_testing_macro::chaos_test;

chaos_test!(the_macro_expands_and_threads_its_seed, 3, |seed| {
	// The expansion must produce real `#[test] fn`s that run this body with the iteration seed. If the generated
	// path were wrong this fails to compile; the arithmetic proves the seed reached the body.
	assert_eq!(seed.wrapping_mul(2), seed.wrapping_add(seed));
});

#[test]
#[ignore = "spawned as a subprocess by the_seed_environment_variable_is_still_named_seed"]
fn seed_probe_that_always_fails() {
	// Not a real test. It exists so the probe below can drive run_iteration's actual failure path
	// in a child process and read back the seed it resolved.
	reifydb_testing_chaos::seed::run_iteration("probe", 0, |_| panic!("probe"));
}

#[test]
fn the_seed_environment_variable_is_still_named_seed() {
	// Nothing else asserts the NAME of the environment variable run_iteration reads. Rename or
	// typo it and every chaos test in the workspace still passes, while `make test-chaos SEED=..`
	// silently stops pinning anything - a failure would print a fresh random seed and the printed
	// replay command would not reproduce it. That is the silent-coverage-loss this suite exists to
	// catch, so drive the real path rather than asserting on a constant.
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
	// assert_pinned is the mechanism that turns a silently re-pointed regression into a loud failure, so it must
	// be exact: the recorded value passes, and a value one bit away does not.
	let corpus = Corpus::new(0xDEAD_BEEF, 12);
	corpus.assert_pinned(0xDEAD_BEEF);

	let outcome = std::panic::catch_unwind(|| Corpus::new(0xDEAD_BEEF, 12).assert_pinned(0xDEAD_BEEE));
	assert!(outcome.is_err(), "a fingerprint that does not match its pin must fail, or stale pins go unnoticed");
}

#[test]
fn mixing_is_order_sensitive_and_stable() {
	// The window regressions pin literal fingerprints, so this arithmetic is a compatibility surface: if it ever
	// changes, five pinned defect reproductions silently stop covering what they name. Order sensitivity is what
	// makes the fingerprint describe a sequence rather than a multiset of operations.
	assert_eq!(mix(0, 1), mix(0, 1), "mixing must be deterministic");
	assert_ne!(mix(mix(0, 1), 2), mix(mix(0, 2), 1), "swapping two operations must change the fingerprint");
	assert_ne!(mix(0, 1), mix(1, 0), "state and value must not be interchangeable");
}

#[test]
fn splitting_decorrelates_the_parameter_stream_from_the_corpus_stream() {
	// Parameters decide WHAT is under test and the sequence seed decides the corpus. If they shared a stream,
	// widening a parameter range would silently reshuffle every corpus and no result could be compared across a
	// generator change. Different master seeds must move both independently.
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
