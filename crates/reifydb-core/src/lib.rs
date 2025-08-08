// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

mod common;
pub mod delta;
pub mod hook;
pub mod index;
pub mod interface;
pub mod result;
pub mod row;
mod sort;
pub mod util;
pub mod value;

pub use common::*;
pub use interface::{
    BorrowedSpan, ColumnDescriptor, IntoOwnedSpan, OwnedSpan, Span, SpanColumn, SpanLine,
};
pub use result::*;
pub use row::{EncodedKey, EncodedKeyRange};
pub use sort::{SortDirection, SortKey};
pub use util::{BitVec, CowVec, Either, WaitGroup, retry};
pub use value::{
    Blob, Date, DateTime, GetType, Interval, IntoValue, OrderedF32, OrderedF64, RowId, Time, Type,
    Uuid4, Uuid7, Value,
};
