// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::error::diagnostic::Diagnostic;
use reifydb_type::fragment::Fragment;

/// Subsystem initialization failed
pub fn init_failed(subsystem: impl std::fmt::Display, reason: impl std::fmt::Display) -> Diagnostic {
	Diagnostic {
		code: "SUB_001".to_string(),
		statement: None,
		message: format!("{} subsystem initialization failed: {}", subsystem, reason),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("Check subsystem configuration and dependencies".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Required feature is not enabled
pub fn feature_disabled(feature: impl std::fmt::Display) -> Diagnostic {
	Diagnostic {
		code: "SUB_002".to_string(),
		statement: None,
		message: format!("Required feature '{}' is not enabled", feature),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("Enable the required feature in Cargo.toml".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Server socket binding failed
pub fn bind_failed(addr: impl std::fmt::Display, reason: impl std::fmt::Display) -> Diagnostic {
	Diagnostic {
		code: "SUB_003".to_string(),
		statement: None,
		message: format!("Failed to bind to {}: {}", addr, reason),
		column: None,
		fragment: Fragment::None,
		label: Some("Check if address is already in use or permissions are insufficient".to_string()),
		help: Some("Try a different port or check firewall settings".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Graceful shutdown failed
pub fn shutdown_failed(subsystem: impl std::fmt::Display, reason: impl std::fmt::Display) -> Diagnostic {
	Diagnostic {
		code: "SUB_004".to_string(),
		statement: None,
		message: format!("{} subsystem shutdown failed: {}", subsystem, reason),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Failed to retrieve local address after binding
pub fn address_unavailable(reason: impl std::fmt::Display) -> Diagnostic {
	Diagnostic {
		code: "SUB_005".to_string(),
		statement: None,
		message: format!("Failed to get local address: {}", reason),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Socket configuration failed
pub fn socket_config_failed(reason: impl std::fmt::Display) -> Diagnostic {
	Diagnostic {
		code: "SUB_006".to_string(),
		statement: None,
		message: format!("Socket configuration failed: {}", reason),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: Some("Check socket options and system limits".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}
