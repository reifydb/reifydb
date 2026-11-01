// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Pinned corpora for defects the sweeps already found. Ungated on purpose: a recorded defect must
// re-run on every `cargo test`, not only when the chaos feature is on. The allow is scoped to this
// binary rather than the shared modules, so the feature-on build still reports what is dead there.
#![allow(dead_code)]

#[path = "chaos/framework/mod.rs"]
mod framework;
#[path = "chaos/operators/mod.rs"]
mod operators;

use crate::operators::sink::{Kind, Layout, drive, params};

#[test]
fn sink_ring_evicting_unpartitioned_seed_18016519944128671930() {
	// A delete must free its ring slot, otherwise an insert evicts a live row while the ring has room.
	drive(
		18016519944128671930,
		params(
			Kind::Ring {
				capacity: 4,
			},
			Layout::Unpartitioned,
		),
	);
}

#[test]
fn sink_ring_capacity_1_seed_18394470728351882031() {
	// An update to an evicted row must not bring it back, otherwise it evicts a newer row.
	drive(
		18394470728351882031,
		params(
			Kind::Ring {
				capacity: 1,
			},
			Layout::Partitioned,
		),
	);
}
