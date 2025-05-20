// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![deny(clippy::unwrap_used)]
// #![deny(clippy::expect_used)]

pub use engine::Engine;
pub use error::Error;

mod engine;
mod error;
pub mod execute;
mod function;

pub type Result<T> = std::result::Result<T, Error>;
