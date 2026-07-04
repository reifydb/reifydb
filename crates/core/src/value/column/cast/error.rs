// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	error::{Diagnostic, Error, IntoDiagnostic},
	fragment::Fragment,
	value::value_type::ValueType,
};

#[derive(Debug, thiserror::Error)]
pub enum CastError {
	#[error("unsupported cast from {from_type} to {to_type}")]
	UnsupportedCast {
		fragment: Fragment,
		from_type: ValueType,
		to_type: ValueType,
	},

	#[error("failed to cast to {target}")]
	InvalidNumber {
		fragment: Fragment,
		target: ValueType,
		cause: Diagnostic,
	},

	#[error("failed to cast to bool")]
	InvalidBoolean {
		fragment: Fragment,
		cause: Diagnostic,
	},

	#[error("failed to cast to {target}")]
	InvalidUuid {
		fragment: Fragment,
		target: ValueType,
		cause: Diagnostic,
	},

	#[error("failed to cast to {target}")]
	InvalidTemporal {
		fragment: Fragment,
		target: ValueType,
		cause: Diagnostic,
	},

	#[error("failed to cast BLOB to UTF8")]
	InvalidBlobToUtf8 {
		fragment: Fragment,
		cause: Diagnostic,
	},
}

impl IntoDiagnostic for CastError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			CastError::UnsupportedCast { fragment, from_type, to_type } => {
				let label = Some(format!("cannot cast {} of type {} to {}", fragment.text(), from_type, to_type));
				Diagnostic {
					code: "CAST_001".to_string(),
					rql: None,
					message: format!("unsupported cast from {} to {}", from_type, to_type),
					fragment,
					label,
					help: Some("ensure the source and target types are compatible for casting".to_string()),
					notes: vec!["supported casts include: numeric to numeric, string to temporal, boolean to numeric"
						.to_string()],
					column: None,
					cause: None,
					operator_chain: None,
				}
			}
			CastError::InvalidNumber { fragment, target, cause } => Diagnostic {
				code: "CAST_002".to_string(),
				rql: None,
				message: format!("failed to cast to {}", target),
				fragment,
				label: Some(format!("failed to cast to {}", target)),
				help: None,
				notes: vec![],
				column: None,
				cause: Some(Box::from(cause)),
				operator_chain: None,
			},
			CastError::InvalidBoolean { fragment, cause } => Diagnostic {
				code: "CAST_004".to_string(),
				rql: None,
				message: "failed to cast to bool".to_string(),
				fragment,
				label: Some("failed to cast to bool".to_string()),
				help: None,
				notes: vec![],
				column: None,
				cause: Some(Box::from(cause)),
				operator_chain: None,
			},
			CastError::InvalidUuid { fragment, target, cause } => Diagnostic {
				code: "CAST_005".to_string(),
				rql: None,
				message: format!("failed to cast to {}", target),
				fragment,
				label: Some(format!("failed to cast to {}", target)),
				help: None,
				notes: vec![],
				column: None,
				cause: Some(Box::from(cause)),
				operator_chain: None,
			},
			CastError::InvalidTemporal { fragment, target, cause } => Diagnostic {
				code: "CAST_003".to_string(),
				rql: None,
				message: format!("failed to cast to {}", target),
				fragment,
				label: Some(format!("failed to cast to {}", target)),
				help: None,
				notes: vec![],
				column: None,
				cause: Some(Box::from(cause)),
				operator_chain: None,
			},
			CastError::InvalidBlobToUtf8 { fragment, cause } => Diagnostic {
				code: "CAST_006".to_string(),
				rql: None,
				message: "failed to cast BLOB to UTF8".to_string(),
				fragment,
				label: Some("failed to cast BLOB to UTF8".to_string()),
				help: Some("BLOB contains invalid UTF-8 bytes. Consider using to_utf8_lossy() function instead"
					.to_string()),
				notes: vec![],
				column: None,
				cause: Some(Box::from(cause)),
				operator_chain: None,
			},
		}
	}
}

impl From<CastError> for Error {
	fn from(err: CastError) -> Self {
		Error(Box::new(err.into_diagnostic()))
	}
}
