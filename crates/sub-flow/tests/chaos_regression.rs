// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Pinned corpora for defects the sweeps already found. Ungated on purpose: a recorded defect must
// re-run on every `cargo test`, not only when the chaos feature is on.
//
// This binary shares the whole chaos module tree with chaos.rs but only calls `drive` with explicit
// parameters, so everything the random sweeps need - framework::fuzz, each operator's drive_random -
// is legitimately unused here. The allow is scoped to this binary rather than to the shared modules
// so that the feature-on build of chaos.rs still reports anything that is dead everywhere.
#![allow(dead_code)]

#[path = "chaos/framework/mod.rs"]
mod framework;
#[path = "chaos/operators/mod.rs"]
mod operators;
