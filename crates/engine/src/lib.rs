// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

pub use engine::Engine;
pub use error::Error;
pub use execute::{execute_tx, execute_rx};

mod engine;
mod error;
mod evaluate;
pub(crate) mod execute;
pub mod frame;
#[allow(dead_code)]
mod function;
mod get;
mod system;
#[allow(dead_code)]
pub(crate) mod view;

pub type Result<T> = std::result::Result<T, Error>;
