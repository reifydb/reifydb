// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub use error::Error;

pub mod binary;
pub mod bincode;
mod error;
pub mod format;
pub mod keycode;

pub type Result<T> = std::result::Result<T, Error>;
