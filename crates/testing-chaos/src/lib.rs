// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Chaos-testing substrate shared by every chaos suite: seed derivation with pinned replay,
//! parameter-space fuzzing, and corpus fingerprints.
//!
//! Only pieces carrying no domain semantics live here. Drivers, oracles and harnesses stay
//! with their domain because their shapes genuinely differ, and forcing them onto one driver
//! would weaken the strongest of them.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod corpus;
pub mod fd;
pub mod fuzz;
#[cfg(feature = "operator")]
pub mod operator;
pub mod seed;
