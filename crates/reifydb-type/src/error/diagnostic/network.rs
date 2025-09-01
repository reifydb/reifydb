// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use crate::error::diagnostic::Diagnostic;
use crate::fragment::OwnedFragment;

/// Network connection error occurred
pub fn connection_error(message: String) -> Diagnostic {
	Diagnostic {
		code: "NET_001".to_string(),
		statement: None,
		message: format!("Connection error: {}", message),
		column: None,
		fragment: OwnedFragment::None,
		label: None,
		help: Some("Check network connectivity and server status"
			.to_string()),
		notes: vec![],
		cause: None,
	}
}

/// Engine processing error on the network layer
pub fn engine_error(message: String) -> Diagnostic {
	Diagnostic {
		code: "NET_002".to_string(),
		statement: None,
		message: format!("Engine error: {}", message),
		column: None,
		fragment: OwnedFragment::None,
		label: None,
		help: None,
		notes: vec![],
		cause: None,
	}
}

/// gRPC transport layer error
pub fn transport_error(err: impl std::fmt::Display) -> Diagnostic {
	Diagnostic {
		code: "NET_003".to_string(),
		statement: None,
		message: format!("Transport error: {}", err),
		column: None,
		fragment: OwnedFragment::None,
		label: None,
		help: Some("Check network connectivity".to_string()),
		notes: vec![],
		cause: None,
	}
}

/// gRPC status error
pub fn status_error(err: impl std::fmt::Display) -> Diagnostic {
	Diagnostic {
		code: "NET_004".to_string(),
		statement: None,
		message: format!("gRPC status error: {}", err),
		column: None,
		fragment: OwnedFragment::None,
		label: None,
		help: Some("Check gRPC service status".to_string()),
		notes: vec![],
		cause: None,
	}
}
