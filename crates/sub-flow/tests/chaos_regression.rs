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
