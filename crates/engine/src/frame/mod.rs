// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub use column::{FrameColumn, FrameColumnLayout, ColumnValues, Push};
pub use error::Error;
pub use frame::Frame;
pub use layout::FrameLayout;
pub use reference::{RowRef, ValueRef};

mod column;
mod display;
mod error;
mod frame;
mod iterator;
mod layout;
mod reference;
mod transform;
mod view;

pub type Result<T> = std::result::Result<T, Error>;
