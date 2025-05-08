// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]

pub use engine::{Engine, EngineMut, Key, ScanIterator, Value};
pub use error::Error;
pub use memory::{Memory, MemoryScanIterator};
use std::result;

mod engine;
mod error;
mod memory;

pub type Result<T> = result::Result<T, Error>;
