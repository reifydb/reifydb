// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Represents all errors that can occur within the low-level storage layer of ReifyDB.
///
/// This storage layer provides a simple but powerful key-value abstraction over which the
/// higher-level RQL engine operates. It is responsible for data persistence, consistency,
/// and efficient access patterns. This error type encapsulates everything that can go wrong
/// when interacting with the key-value store—whether in-memory or persisted to disk.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Error {}

impl Display for Error {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl std::error::Error for Error {}
