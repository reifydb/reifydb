// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use base::encoding;

/// Represents all possible errors that can occur within the ReifyDB system.
///
/// This unified error type aggregates failures across the different layers of the system,
/// including encoding, query processing (RQL), and low-level storage. It enables consistent
/// error propagation and simplifies error handling across subsystems.
///
/// # Variants
///
/// - `Encoding`: An error occurred during serialization or deserialization of data.  
///
/// - `RQL`: A failure occurred in the Reify Query Language layer.  
///   This can include parsing errors, logical plan issues, optimization failures, or runtime execution faults.
///
/// - `Storage`: A low-level storage engine error was encountered.  
///   This includes I/O errors, key-value corruption, encoding issues at the storage level, or internal storage bugs.
///
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Error {
    /// encoding related error
    Encoding(encoding::Error),
    /// RQL related error
    RQL(rql::Error),
    /// storage related error
    Storage(storage::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Encoding(err) => f.write_fmt(format_args!("encoding error: {}", err)),
            Error::RQL(err) => f.write_fmt(format_args!("rql error: {}", err)),
            Error::Storage(err) => f.write_fmt(format_args!("storage error: {}", err)),
        }
    }
}

impl std::error::Error for Error {}

impl From<encoding::Error> for Error {
    fn from(value: encoding::Error) -> Self {
        Self::Encoding(value)
    }
}

impl From<rql::Error> for Error {
    fn from(value: rql::Error) -> Self {
        Self::RQL(value)
    }
}

impl From<storage::Error> for Error {
    fn from(value: storage::Error) -> Self {
        Self::Storage(value)
    }
}
