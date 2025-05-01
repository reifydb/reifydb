// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use error::Error;

mod error;
mod keyword;
mod lexer;
mod operator;
mod separator;

pub type Result<T> = std::result::Result<T, Error>;
