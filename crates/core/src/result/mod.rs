// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub mod error;
pub mod frame;

pub use error::*;
pub use frame::*;

pub type Result<T> = std::result::Result<T, Error>;
