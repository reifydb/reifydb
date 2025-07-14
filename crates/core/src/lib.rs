// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

pub use bitvec::BitVec;
pub use cowvec::CowVec;
pub use diagnostic::{Diagnostic, DiagnosticColumn};
pub use error::Error;
pub use key::{EncodedKey, EncodedKeyRange};
pub use data_type::{GetDataType, DataType};
pub use sort::{SortDirection, SortKey};
pub use retry::retry;
pub use span::{IntoSpan, SpanLine, SpanColumn, Span};
pub use value::Value;
pub use version::Version;

mod bitvec;
pub mod clock;
mod cowvec;
pub mod delta;
mod diagnostic;
pub mod either;
pub mod encoding;
mod error;
pub mod hook;
pub mod interface;
mod key;
mod data_type;
pub mod num;
mod sort;
mod retry;
pub mod row;
mod span;
mod value;
mod version;
pub mod wait_group;

#[derive(Copy, Clone, Debug)]
pub enum StoreKind {
    DeferredView,
    Series,
    Table,
}
