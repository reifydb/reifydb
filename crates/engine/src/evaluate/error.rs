// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_diagnostic::Diagnostic;
use std::fmt::{Display, Formatter};

#[derive(Debug, PartialEq)]
pub struct Error(pub Diagnostic);

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // Display::fmt(&self.0, f)
        todo!()
    }
}

impl Error {
    pub fn diagnostic(self) -> Diagnostic {
        self.0
    }
}

// impl From<&str> for Error {
//     fn from(value: &str) -> Self {
//         Self(String::from(value))
//     }
// }
//
// impl From<String> for Error {
//     fn from(value: String) -> Self {
//         Self(value)
//     }
// }

impl std::error::Error for Error {}
