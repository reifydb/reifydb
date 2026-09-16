// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{fragment::Fragment, params::Params, value::identity::IdentityId};

const RQL: &str = "let $mk = () { let $g = () { 7 }; $g }; let $h = $mk(); $h()";

#[test]
fn naming_a_closure_where_rows_are_read_says_it_holds_a_closure() {
	// The variable is bound, so reporting it as undefined sends the user to define a name that already exists.
	let t = TestEngine::new();

	let result = t.inner().query_as(IdentityId::system(), RQL, Params::None);

	let Some(err) = result.error else {
		panic!("returning a closure is not supported yet, so this must fail, got {:?}", result.frames);
	};
	let diagnostic = err.diagnostic();
	assert_eq!(diagnostic.code, "RUNTIME_013", "got: {diagnostic:?}");
	assert!(
		!diagnostic.message.contains("is not defined"),
		"the name is bound, so the error must not claim it is undefined, got: {}",
		diagnostic.message
	);
	assert!(diagnostic.message.contains("closure"), "the error must name the cause, got: {}", diagnostic.message);
	assert!(
		matches!(diagnostic.fragment, Fragment::Statement { .. }),
		"the fragment must carry a position, got: {:?}",
		diagnostic.fragment
	);
}
