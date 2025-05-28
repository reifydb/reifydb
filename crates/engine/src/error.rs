// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate;
use std::fmt::{Display, Formatter};

/// Represents all possible errors that can occur within the RQL (Reify Query Language) layer.
///
/// RQL is the high-level query and execution reifydb_engine of ReifyDB, responsible for parsing,
/// planning, optimizing, and executing queries over a low-level key-value store. This error type
/// encapsulates issues encountered at any stage of query lifecycle—from malformed syntax to failed
/// execution.
#[derive(Debug, PartialEq)]
pub enum Error {
    Evaluation(evaluate::Error),
    Frame(frame::Error),
    Transaction(transaction::Error),
}

impl Display for Error {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl From<evaluate::Error> for Error {
    fn from(value: evaluate::Error) -> Self {
        Self::Evaluation(value)
    }
}

impl From<frame::Error> for Error {
    fn from(value: frame::Error) -> Self {
        Self::Frame(value)
    }
}

impl From<transaction::Error> for Error {
    fn from(value: transaction::Error) -> Self {
        Self::Transaction(value)
    }
}

impl std::error::Error for Error {}
