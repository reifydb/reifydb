// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub use error::Error;
pub use interface::{
    BorrowedSpan, ColumnDescriptor, IntoOwnedSpan, OwnedSpan, Span, SpanColumn, SpanLine,
};
pub use row::key::{EncodedKey, EncodedKeyRange};
use serde::{Deserialize, Serialize};
pub use sort::{SortDirection, SortKey};
pub use util::{BitVec, CowVec, Either, WaitGroup, retry};
pub use value::{
    Blob, Date, DateTime, GetType, Interval, OrderedF32, OrderedF64, RowId, Time, Type, Uuid4,
    Uuid7, Value,
};

pub type Result<T> = std::result::Result<T, Error>;

pub mod delta;
pub mod error;
pub mod frame;
pub mod hook;
pub mod interface;
pub mod row;
mod sort;
pub mod util;
pub mod value;

pub type Version = u64;

#[derive(Copy, Clone, Debug)]
pub enum StoreKind {
    ComputedView,
    Series,
    Table,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
}

impl Default for JoinType {
    fn default() -> Self {
        JoinType::Left
    }
}
