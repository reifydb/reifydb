// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::fmt::{Display, Formatter};

#[derive(Debug, PartialEq)]
pub struct Error(pub String); // FIXME diagnostic

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl From<Box<dyn std::error::Error>> for Error {
    fn from(err: Box<dyn std::error::Error>) -> Self {
        Self(err.to_string())
    }
}

impl From<&str> for Error {
    fn from(value: &str) -> Self {
        Self(String::from(value))
    }
}

impl From<String> for Error {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl std::error::Error for Error {}
