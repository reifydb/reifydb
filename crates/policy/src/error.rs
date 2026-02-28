// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
	error::{Diagnostic, Error, IntoDiagnostic},
	fragment::Fragment,
};

#[derive(Debug, thiserror::Error)]
pub enum PolicyError {
	#[error("Policy '{policy_name}' denied {operation} on {target}")]
	PolicyDenied {
		policy_name: String,
		operation: String,
		target: String,
	},

	#[error("No {operation} policy defined on {target}")]
	NoPolicyDefined {
		operation: String,
		target: String,
		target_type: String,
	},

	#[error("{session_type} session denied for identity")]
	SessionDenied {
		session_type: String,
	},
}

impl IntoDiagnostic for PolicyError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			PolicyError::PolicyDenied {
				policy_name,
				operation,
				target,
			} => Diagnostic {
				code: "POLICY_001".to_string(),
				statement: None,
				message: format!("Policy '{}' denied {} on {}", policy_name, operation, target),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("The write operation violates a policy constraint".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			PolicyError::NoPolicyDefined {
				operation,
				target,
				target_type,
			} => Diagnostic {
				code: "POLICY_002".to_string(),
				statement: None,
				message: format!("No {} policy defined for {} on {}", operation, operation, target),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some({
					let article = |word: &str| {
						if matches!(
							word.as_bytes().first(),
							Some(b'a' | b'e' | b'i' | b'o' | b'u')
						) {
							"an"
						} else {
							"a"
						}
					};
					format!(
						"Define {} {} policy with {} {} clause to allow {} operations",
						article(&target_type),
						target_type,
						article(&operation),
						operation,
						operation
					)
				}),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
			PolicyError::SessionDenied {
				session_type,
			} => Diagnostic {
				code: "SESSION_001".to_string(),
				statement: None,
				message: format!("{} session denied for identity", session_type),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("Create a session policy to grant access".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
		}
	}
}

impl From<PolicyError> for Error {
	fn from(err: PolicyError) -> Self {
		Error(err.into_diagnostic())
	}
}
