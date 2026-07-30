// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Chaos-testing substrate shared by every chaos suite in the workspace: per-iteration seed derivation with pinned
//! replay, parameter-space fuzzing, and corpus fingerprints that turn a silently re-pointed regression into a loud
//! failure.
//!
//! Only pieces that carry no domain semantics live here. Workload drivers, oracles and harnesses stay with their
//! domain, because their shapes genuinely differ: a storage suite compares an exact total model across several store
//! configurations, a transaction suite checks invariants over an execution trace, and an operator suite bounds a
//! materialized view from above and below. Forcing those onto one driver would weaken the strongest of them.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod corpus;
pub mod fuzz;
pub mod seed;
