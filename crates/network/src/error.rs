// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::Diagnostic;
use reifydb_diagnostic::DefaultRenderer;
use std::fmt::{Display, Formatter};
use tonic::Status;

#[derive(Debug)]
pub enum NetworkError {
	ConnectionError { message: String },
	EngineError { message: String },
	ExecutionError { source: String, diagnostic: Diagnostic },
}

impl Display for NetworkError {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			NetworkError::ConnectionError { message } => write!(f, "connection error: {}", message),
			NetworkError::EngineError { message } => write!(f, "engine error: {}", message),
			NetworkError::ExecutionError { diagnostic, source } => {
				f.write_str(&DefaultRenderer::render_string(&diagnostic, source))
			}
		}
	}
}

impl std::error::Error for NetworkError {}

impl NetworkError {
	pub fn connection_error(message: impl Into<String>) -> Self {
		Self::ConnectionError { message: message.into() }
	}

	pub fn execution_error(source: &str, diagnostic: Diagnostic) -> Self {
		Self::ExecutionError { source: source.to_string(), diagnostic }
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
		Self::ExecutionError { source: "".to_string(), diagnostic: err.diagnostic() }
	}
}
