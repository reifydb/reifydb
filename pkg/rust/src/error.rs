// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::encoding;
use std::fmt::{Display, Formatter};

#[derive(Debug, PartialEq)]
pub enum Error {
    /// encoding related error
    Encoding(encoding::Error),
    /// RQL related error
    RQL(reifydb_rql::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Encoding(err) => f.write_fmt(format_args!("encoding error: {}", err)),
            Error::RQL(err) => f.write_fmt(format_args!("rql error: {}", err)),
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
