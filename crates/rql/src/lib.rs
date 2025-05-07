// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

pub use error::Error;
pub use expression::Expression;
pub use value::{Value, ValueType};

pub mod ast;
pub mod catalog;
mod error;
mod expression;
pub mod plan;
mod value;
