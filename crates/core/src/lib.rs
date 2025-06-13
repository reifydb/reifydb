// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

pub use cowvec::{r#async::AsyncCowVec, sync::CowVec};
pub use key::{
    EncodableKey, EncodedKey, EncodedKeyRange, Key, KeyKind, SchemaKey, TableKey, TableRowKey,
};
pub use value::{Value, ValueKind};
pub use version::Version;

pub mod catalog;
pub mod clock;
mod cowvec;
pub mod delta;
pub mod either;
pub mod encoding;
pub mod hook;
mod key;
pub mod num;
pub mod ordered_float;
pub mod row;
mod value;
mod version;
pub mod wait_group;

#[derive(Copy, Clone, Debug)]
pub enum StoreKind {
    DeferredView,
    Series,
    Table,
}
