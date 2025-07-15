// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::Diagnostic;
use std::fmt::{Display, Formatter};
use std::ops::Deref;

#[derive(Debug, PartialEq)]
pub struct Error(pub Diagnostic);

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl Error {
    pub fn diagnostic(self) -> Diagnostic {
        self.0
    }
}

impl std::error::Error for Error {}

impl Deref for Error {
    type Target = Diagnostic;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
