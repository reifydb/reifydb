// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Runtime counterpart to RQL's `expression/` planner module: that crate produces the representation, this one
//! runs it. Evaluation works on column buffers wherever possible, so the per-row interpreter cost is paid only
//! when an expression cannot be vectorised.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

use reifydb_value::Result;

pub mod error;
pub mod expression;
pub mod stack;
