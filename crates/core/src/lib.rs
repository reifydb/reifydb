// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

pub use error::Error;
pub use row::key::{EncodedKey, EncodedKeyRange};
pub use sort::{SortDirection, SortKey};
pub use interface::{BorrowedSpan, IntoOwnedSpan, OwnedSpan, Span, SpanColumn, SpanLine};
pub use util::{BitVec, CowVec, Either, WaitGroup, retry};
pub use value::{Date, DateTime, GetType, Interval, OrderedF32, OrderedF64, Time, Type, Value};

// Note: error! and return_error! macros are automatically available due to #[macro_export]

pub mod clock;
pub mod delta;
pub mod error;
pub mod hook;
pub mod interface;
pub mod row;
mod sort;
pub mod util;
pub mod value;

pub type Version = u64;

#[derive(Copy, Clone, Debug)]
pub enum StoreKind {
    DeferredView,
    Series,
    Table,
}
