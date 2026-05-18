// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Runtime expression evaluator. Compiles RQL expressions into a small typed tree that the VM can apply over
//! columns - access, arithmetic, comparison, logical, casts, conversions, lookups, function and routine calls.
//! Evaluation operates on column buffers wherever possible so a per-row interpreter cost is paid only when an
//! expression cannot be vectorised.
//!
//! This module is the engine-side counterpart to RQL's `expression/` planner module: that crate produces the
//! expression representation, this one runs it.

pub mod access;
pub mod arith;
pub mod call;
pub mod cast;
pub mod compare;
pub mod compile;
pub(crate) mod constant;
pub mod context;
pub mod convert;
pub mod eval;
pub(crate) mod logic;
pub mod lookup;
pub(crate) mod option;
pub mod parameter;
pub mod prefix;
pub mod scalar;
pub(crate) mod udf_extract;
