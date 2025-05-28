// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use std::fmt::{Display, Formatter};

/// Represents all errors that can occur within the low-level store layer of ReifyDB.
///
/// This store layer provides a simple but powerful key-value abstraction over which the
/// higher-level RQL reifydb_engine operates. It is responsible for data persistence, consistency,
/// and efficient access patterns. This error type encapsulates everything that can go wrong
/// when interacting with the key-value store—whether in-memory or persisted to disk.
#[derive(Debug, PartialEq)]
pub enum Error {}

impl Display for Error {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl std::error::Error for Error {}
