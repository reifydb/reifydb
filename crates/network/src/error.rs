// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::diagnostic::{DefaultRenderer, Diagnostic};
use std::fmt::{Display, Formatter};
use tonic::Status;

#[derive(Debug)]
pub enum NetworkError {
    ConnectionError { message: String },
    EngineError { message: String },
    ExecutionError { diagnostic: Diagnostic },
}

impl Display for NetworkError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NetworkError::ConnectionError { message } => write!(f, "connection error: {}", message),
            NetworkError::EngineError { message } => write!(f, "engine error: {}", message),
            NetworkError::ExecutionError { diagnostic } => {
                f.write_str(&DefaultRenderer::render_string(&diagnostic))
            }
        }
    }
}

impl std::error::Error for NetworkError {}

impl NetworkError {
    pub fn connection_error(message: impl Into<String>) -> Self {
        Self::ConnectionError { message: message.into() }
    }

    pub fn execution_error(diagnostic: Diagnostic) -> Self {
        Self::ExecutionError { diagnostic }
    }
}

impl From<Status> for NetworkError {
    fn from(err: Status) -> Self {
        Self::ConnectionError { message: err.to_string() }
    }
}

impl From<tonic::transport::Error> for NetworkError {
    fn from(err: tonic::transport::Error) -> Self {
        Self::ConnectionError { message: err.to_string() }
    }
}

impl From<reifydb_engine::Error> for NetworkError {
    fn from(err: reifydb_engine::Error) -> Self {
        Self::EngineError { message: err.to_string() }
    }
}

impl From<reifydb_core::Error> for NetworkError {
    fn from(err: reifydb_core::Error) -> Self {
        Self::ExecutionError { diagnostic: err.diagnostic() }
    }
}
