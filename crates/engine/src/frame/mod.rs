// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#[cfg(test)]
pub use column::AsSlice;
pub use column::{Column, ColumnValues};
pub use error::Error;
pub use frame::Frame;
pub use lazy::LazyFrame;
pub use reference::{RowRef, ValueRef};

pub mod aggregate;
mod column;
mod display;
mod error;
mod frame;
mod iterator;
mod lazy;
mod reference;
mod transform;
mod view;

pub type Result<T> = std::result::Result<T, Error>;
