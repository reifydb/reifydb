// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

pub use engine::{Engine, Transaction, TransactionMut};
pub use error::Error;

mod engine;
mod error;
pub mod execute;
pub mod mvcc;
mod session;
pub mod svl;

pub type Result<T> = std::result::Result<T, Error>;
