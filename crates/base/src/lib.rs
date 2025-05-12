// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

pub use catalog::*;
pub use error::Error;
pub use key::{Key, KeyError};
pub use label::Label;
pub use row::{Row, RowIter, RowIterator};
pub use value::{Value, ValueType};

mod catalog;
pub mod encoding;
mod error;
pub mod expression;
mod key;
mod label;
mod row;
pub mod schema;
mod value;

pub type Result<T> = std::result::Result<T, Error>;
