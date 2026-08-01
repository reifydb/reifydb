// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Runtime expression evaluator, the engine-side counterpart to RQL's `expression/` planner module: that crate
//! produces the representation, this one runs it. Evaluation works on column buffers wherever possible, so the
//! per-row interpreter cost is paid only when an expression cannot be vectorised.

pub mod access;
pub mod arith;
pub mod call;
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
