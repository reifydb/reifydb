// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Engine-side runner for first-class CREATE TEST / RUN TESTS. Compiles the test body in the active admin
//! transaction and executes it; collects per-test results into the typed `result/` types the caller observes.
//! Tests run inside the same transaction as the admin command, so test side effects are visible until the outer
//! transaction commits or rolls back.

pub mod result;
pub(crate) mod run;
