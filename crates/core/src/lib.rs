// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

pub use bitvec::BitVec;
pub use cowvec::{r#async::AsyncCowVec, sync::CowVec};
pub use key::{EncodedKey, EncodedKeyRange};
pub use kind::{GetKind, Kind};
pub use retry::retry;
pub use sort::{SortDirection, SortKey};
pub use value::Value;
pub use version::Version;

mod bitvec;
pub mod catalog;
pub mod clock;
mod cowvec;
pub mod delta;
pub mod either;
pub mod encoding;
pub mod hook;
mod key;
mod kind;
pub mod num;
pub mod ordered_float;
mod retry;
pub mod row;
mod sort;
mod value;
mod version;
pub mod wait_group;

#[derive(Copy, Clone, Debug)]
pub enum StoreKind {
    DeferredView,
    Series,
    Table,
}
