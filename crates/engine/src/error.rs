// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{evaluate, execute, frame};
use reifydb_core::Diagnostic;
use reifydb_rql::ast;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum Error {
    Ast(ast::Error),
    Catalog(reifydb_core::Error), // FIXME this is broken just a quick hack for now
    Evaluation(evaluate::Error),
    Execution(execute::Error),
    Frame(frame::Error),
    Transaction(reifydb_transaction::Error),
}

impl Error {
    pub fn execution(diagnostic: Diagnostic) -> Self {
        Self::Execution(execute::Error(diagnostic))
    }
}

impl Display for Error {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl From<ast::Error> for Error {
    fn from(err: ast::Error) -> Self {
        Self::Ast(err)
    }
}

impl From<evaluate::Error> for Error {
    fn from(err: evaluate::Error) -> Self {
        Self::Evaluation(err)
    }
}

impl From<execute::Error> for Error {
    fn from(err: execute::Error) -> Self {
        Self::Execution(err)
    }
}

impl From<frame::Error> for Error {
    fn from(err: frame::Error) -> Self {
        Self::Frame(err)
    }
}

impl From<reifydb_transaction::Error> for Error {
    fn from(err: reifydb_transaction::Error) -> Self {
        Self::Transaction(err)
    }
}

impl std::error::Error for Error {}

impl Error {
    pub fn diagnostic(self) -> Diagnostic {
        match self {
            Error::Ast(err) => err.diagnostic(),
            Error::Catalog(err) => err.diagnostic(),
            Error::Evaluation(err) => err.diagnostic(),
            Error::Execution(err) => err.diagnostic(),
            Error::Frame(_) => unimplemented!(),
            Error::Transaction(_) => unimplemented!(),
        }
    }
}

impl From<Error> for reifydb_core::Error {
    fn from(err: Error) -> Self {
        Self(err.diagnostic())
    }
}

// FIXME
impl From<reifydb_core::Error> for Error {
    fn from(err: reifydb_core::Error) -> Self {
        Self::Catalog(err)
    }
}
