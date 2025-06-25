// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use std::fmt::{Display, Formatter};
use reifydb_diagnostic::Diagnostic;

#[derive(Debug, PartialEq)]
pub struct Error(pub Diagnostic);

impl Display for Error {
	fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
		todo!()
	}
}

impl Error {
    pub fn diagnostic(self) -> Diagnostic {
        self.0
    }
}

impl std::error::Error for Error {}
