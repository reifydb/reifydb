// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{lex, parse};
use reifydb_core::diagnostic::Diagnostic;
use std::fmt::{Display, Formatter};

#[derive(Debug, PartialEq)]
pub enum Error {
    Lex(lex::Error),
    Parse(parse::Error),
}

impl Display for Error {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl std::error::Error for Error {}

impl From<lex::Error> for Error {
    fn from(value: lex::Error) -> Self {
        Self::Lex(value)
    }
}

impl From<parse::Error> for Error {
    fn from(value: parse::Error) -> Self {
        Self::Parse(value)
    }
}

impl Error {
    pub fn diagnostic(self) -> Diagnostic {
        match self {
            Error::Lex(_) => unimplemented!(),
            Error::Parse(err) => err.diagnostic(),
        }
    }
}
