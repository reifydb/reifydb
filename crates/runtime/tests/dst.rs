// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Integration tests for the DST (Deterministic Software Testing) actor system.
//!
//! This entire test binary is compiled only when `REIFYDB_DST=1` is set,
//! which activates the `reifydb_target = "dst"` cfg flag.

#![cfg(reifydb_target = "dst")]

mod dst {
	pub mod helpers;

	mod determinism;
	mod lifecycle;
	mod ordering;
	mod scope;
	mod timers;
}
