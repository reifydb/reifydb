// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub mod frame;

pub use frame::*;
pub use reifydb_type::{Error, diagnostic, err, error, return_error, return_internal_error};

pub type Result<T> = std::result::Result<T, Error>;
