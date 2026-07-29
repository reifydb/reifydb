// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::operators::window::sliding::{Params, drive};

#[test]
fn a_seal_does_not_wipe_every_sliding_window_at_once() {
	// Recorded from a chaos failure: make test-chaos SEED=7679903394466761495
	// FILTER=window_sliding_grace_chaos_0
	//
	// A single seal at ledger 150658 withdrew all thirty live windows across three groups, and
	// every later event was refused as late. Sliding identified a window by its SLIDE INDEX
	// rather than by its start coordinate, so span.start was a small integer (0..40) while the
	// admission gate, the expiry index and the seal timer all read it as milliseconds. Once the
	// ledger passed size + grace, seal_instant(index, cutoff) sat below it for every window in
	// existence, so they all sealed together regardless of where in time they actually were.
	//
	// The corpus spans 400s with a 30s/10s window, so each coordinate belongs to three windows;
	// that multiplicity is why one unit mismatch takes out thirty rows at once.
	let corpus = drive(
		7_679_903_394_466_761_495,
		Params {
			size_secs: 30,
			slide_secs: 10,
			grace_secs: 45,
			groups: 3,
			steps: 60,
			max_batch: 4,
			coord_span_ms: 400_000,
			remove_pct: 25,
			update_pct: 15,
			seal_pct: 30,
		},
	);
	corpus.assert_pinned(0x4878_559b_c25b_2555);
}
