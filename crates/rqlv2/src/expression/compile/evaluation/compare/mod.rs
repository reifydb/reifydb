// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Comparison operations module.
//!
//! This module implements comparison operations (Eq, Ne, Gt, Ge, Lt, Le)
//! following the pattern from the old engine implementation with explicit
//! type matching for all supported type combinations.
//!
//! Type support:
//! - Numeric types: All combinations supported with type promotion
//! - Temporal types: Same-type only (Date, DateTime, Time, Duration)
//! - Text types: Lexicographic ordering
//! - UUID types: Same-type only (Uuid4, Uuid7)
//! - Boolean: Only equality (Eq, Ne), not ordered comparisons

pub mod eq;
pub mod ge;
pub mod gt;
pub mod le;
pub mod lt;
pub mod ne;

pub(crate) use eq::eval_eq;
pub(crate) use ge::eval_ge;
pub(crate) use gt::eval_gt;
pub(crate) use le::eval_le;
pub(crate) use lt::eval_lt;
pub(crate) use ne::eval_ne;
