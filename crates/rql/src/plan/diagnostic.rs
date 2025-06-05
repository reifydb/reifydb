// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::plan::Error;
use reifydb_diagnostic::Diagnostic;

impl Error {
    pub fn diagnostic(self) -> Diagnostic {
        match self {
            Error::InvalidType { got } => reifydb_diagnostic::plan::invalid_type(got),
        }
    }
}
