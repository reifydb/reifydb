// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

pub use error::Error;
pub use explain::{ExplainAst, ExplainLex, ExplainLogicalPlan, ExplainPhysicalPlan};

pub type Result<T> = std::result::Result<T, Error>;

pub mod ast;
mod error;
mod explain;
pub mod expression;
pub mod plan;
