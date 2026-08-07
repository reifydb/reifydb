// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// This file used to assert that a view's own time declaration was reconciled against its source's,
// including through a chain of views. Views no longer declare anything: #time is stamped once at
// the source and every downstream view inherits it verbatim.
//
// So the interesting question is no longer "is the declaration consistent" but "does the value
// actually survive the chain", and that has to be asserted where flows really run - it lives in
// reifydb-sub-flow's time_propagation.rs. What stays here is the definition-time half: a chain of
// views must be creatable without any declaration anywhere.

use reifydb_engine::test_harness::TestEngine;
use reifydb_value::{params::Params, value::identity::IdentityId};

fn event_source_chain() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE cv");
	t.admin("CREATE TABLE cv::src { id: int4, at: datetime } WITH { time: event(at) }");
	t.admin("CREATE DEFERRED VIEW cv::upstream { id: int4, at: datetime } AS { FROM cv::src }");
	t.admin("CREATE DEFERRED VIEW cv::downstream { id: int4, at: datetime } AS { FROM cv::upstream }");
	t
}

#[test]
fn a_view_over_a_view_needs_no_declaration_to_be_created() {
	// Chains of views are the ordinary shape of a pipeline. Nothing about the time domain may
	// stand in the way of creating one, in either domain.
	let t = event_source_chain();
	assert!(
		t.inner()
			.admin_as(
				IdentityId::system(),
				"CREATE DEFERRED VIEW cv::third { id: int4, at: datetime } AS { FROM cv::downstream }",
				Params::None,
			)
			.error
			.is_none(),
		"a third link must create without any declaration anywhere"
	);
}
