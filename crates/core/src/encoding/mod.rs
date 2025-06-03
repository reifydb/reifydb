// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use error::Error;
pub use key::Key;
// pub use value::Value;

pub mod binary;
pub mod bincode;
mod error;
pub mod format;
mod key;
pub mod keycode;
mod value;

pub type Result<T> = std::result::Result<T, Error>;
