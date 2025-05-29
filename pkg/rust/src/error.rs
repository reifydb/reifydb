// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::encoding;
use std::fmt::{Display, Formatter};

/// Represents all possible errors that can occur within the ReifyDB system.
///
/// This unified error type aggregates failures across the different layers of the system,
/// including encoding, query processing (RQL), and low-level store. It enables consistent
/// error propagation and simplifies error handling across subsystems.
///
/// # Variants
///
/// - `Encoding`: An error occurred during serialization or deserialization of data.  
///
/// - `RQL`: A failure occurred in the Reify Query Language layer.  
///   This can include parsing errors, logical plan issues, optimization failures, or runtime execution faults.
///
/// - `Store`: A low-level store engine error was encountered.
///   This includes I/O errors, key-value corruption, encoding issues at the store level, or internal store bugs.
///
#[derive(Debug, PartialEq)]
pub enum Error {
    /// encoding related error
    Encoding(encoding::Error),
    /// RQL related error
    RQL(reifydb_rql::Error),
    /// Persistence related error
    Persistence(reifydb_persistence::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Encoding(err) => f.write_fmt(format_args!("encoding error: {}", err)),
            Error::RQL(err) => f.write_fmt(format_args!("rql error: {}", err)),
            Error::Persistence(err) => f.write_fmt(format_args!("persistence error: {}", err)),
        }
    }
}

impl std::error::Error for Error {}

impl From<encoding::Error> for Error {
    fn from(value: encoding::Error) -> Self {
        Self::Encoding(value)
    }
}

impl From<reifydb_rql::Error> for Error {
    fn from(value: reifydb_rql::Error) -> Self {
        Self::RQL(value)
    }
}

impl From<reifydb_persistence::Error> for Error {
    fn from(value: reifydb_persistence::Error) -> Self {
        Self::Persistence(value)
    }
}
