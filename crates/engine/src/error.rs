// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate;
use reifydb_diagnostic::Diagnostic;
use reifydb_rql::{ast, plan};
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum Error {
    Ast(ast::Error),
    Catalog(reifydb_catalog::Error),
    Evaluation(evaluate::Error),
    Frame(reifydb_frame::Error),
    Plan(plan::Error),
    Policy(reifydb_catalog::PolicyError),
    Transaction(reifydb_transaction::Error),
}

impl Display for Error {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl From<ast::Error> for Error {
    fn from(value: ast::Error) -> Self {
        Self::Ast(value)
    }
}

impl From<reifydb_catalog::Error> for Error {
    fn from(value: reifydb_catalog::Error) -> Self {
        Self::Catalog(value)
    }
}

impl From<evaluate::Error> for Error {
    fn from(value: evaluate::Error) -> Self {
        Self::Evaluation(value)
    }
}

impl From<reifydb_frame::Error> for Error {
    fn from(value: reifydb_frame::Error) -> Self {
        Self::Frame(value)
    }
}

impl From<plan::Error> for Error {
    fn from(value: plan::Error) -> Self {
        Self::Plan(value)
    }
}

impl From<reifydb_catalog::PolicyError> for Error {
    fn from(value: reifydb_catalog::PolicyError) -> Self {
        Self::Policy(value)
    }
}

impl From<reifydb_transaction::Error> for Error {
    fn from(value: reifydb_transaction::Error) -> Self {
        Self::Transaction(value)
    }
}

impl std::error::Error for Error {}

impl Error {
    pub fn diagnostic(self) -> Diagnostic {
        match self {
            Error::Ast(err) => err.diagnostic(),
            Error::Catalog(_) => unimplemented!(),
            Error::Evaluation(err) => err.diagnostic(),
            Error::Frame(_) => unimplemented!(),
            Error::Plan(err) => err.diagnostic(),
            Error::Policy(err) => err.diagnostic(),
            Error::Transaction(_) => unimplemented!(),
        }
    }
}
