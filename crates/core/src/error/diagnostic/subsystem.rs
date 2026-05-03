// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::fmt;

use reifydb_type::{error::Diagnostic, fragment::Fragment};

pub fn init_failed(subsystem: impl fmt::Display, reason: impl fmt::Display) -> Diagnostic {
	Diagnostic {
		code: "SUB_001".to_string(),
		rql: None,
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

pub fn feature_disabled(feature: impl fmt::Display) -> Diagnostic {
	Diagnostic {
		code: "SUB_002".to_string(),
		rql: None,
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

pub fn bind_failed(addr: impl fmt::Display, reason: impl fmt::Display) -> Diagnostic {
	Diagnostic {
		code: "SUB_003".to_string(),
		rql: None,
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

pub fn shutdown_failed(subsystem: impl fmt::Display, reason: impl fmt::Display) -> Diagnostic {
	Diagnostic {
		code: "SUB_004".to_string(),
		rql: None,
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

pub fn address_unavailable(reason: impl fmt::Display) -> Diagnostic {
	Diagnostic {
		code: "SUB_005".to_string(),
		rql: None,
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

pub fn socket_config_failed(reason: impl fmt::Display) -> Diagnostic {
	Diagnostic {
		code: "SUB_006".to_string(),
		rql: None,
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
