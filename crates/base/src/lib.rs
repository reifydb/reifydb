// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

pub use error::Error;
pub use key::{Key, KeyError, SortDirection, SortKey};
pub use row::{Row, RowIter, RowIterator};
pub use row_meta::RowMeta;
pub use value::{Value, ValueKind};

pub mod encoding;
mod error;
pub mod expression;
mod key;
pub mod ordered_float;
mod row;
mod row_meta;
mod value;

pub type Result<T> = std::result::Result<T, Error>;
