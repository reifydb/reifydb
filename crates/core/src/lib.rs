// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

pub use cowvec::{r#async::AsyncCowVec, sync::CowVec};
pub use error::Error;
pub use key::{Key, KeyRange};
pub use key::{SortDirection, SortKey};
pub use row::{Row, RowIter, RowIterator, deserialize_row, serialize_row};
pub use row_meta::RowMeta;
pub use value::{Value, ValueKind};
pub use version::Version;

pub mod clock;
mod cowvec;
pub mod delta;
pub mod either;
pub mod encoding;
mod error;
pub mod hook;
mod key;
pub mod num;
pub mod ordered_float;
mod row;
mod row_meta;
mod value;
mod version;
pub mod wait_group;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Copy, Clone, Debug)]
pub enum StoreKind {
    DeferredView,
    Series,
    Table,
}
