// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

//! BLOB-related diagnostic functions

use std::str::Utf8Error;

use crate::{error::diagnostic::Diagnostic, fragment::Fragment};

/// Invalid hexadecimal string in BLOB constructor
pub fn invalid_hex_string(fragment: Fragment) -> Diagnostic {
	let value = fragment.text();
	Diagnostic {
		code: "BLOB_001".to_string(),
		statement: None,
		message: format!("Invalid hexadecimal string: '{}'", value),
		column: None,
		fragment,
		label: Some("Invalid hex characters found".to_string()),
		help: Some("Hex strings should only contain 0-9, a-f, A-F characters".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Invalid base64 string in BLOB constructor
pub fn invalid_base64_string(fragment: Fragment) -> Diagnostic {
	let value = fragment.text();
	Diagnostic {
		code: "BLOB_002".to_string(),
		statement: None,
		message: format!("Invalid base64 string: '{}'", value),
		column: None,
		fragment,
		label: Some("Invalid base64 encoding found".to_string()),
		help: Some("Base64 strings should only contain A-Z, a-z, 0-9, +, / and = padding".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Invalid base64url string in BLOB constructor
pub fn invalid_base64url_string(fragment: Fragment) -> Diagnostic {
	let value = fragment.text();
	Diagnostic {
		code: "BLOB_003".to_string(),
		statement: None,
		message: format!("Invalid base64url string: '{}'", value),
		column: None,
		fragment,
		label: Some("Invalid base64url encoding found".to_string()),
		help: Some("Base64url strings should only contain A-Z, a-z, 0-9, -, _ characters".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Invalid base58 string in BLOB constructor
pub fn invalid_base58_string(fragment: Fragment) -> Diagnostic {
	let value = fragment.text();
	Diagnostic {
		code: "BLOB_005".to_string(),
		statement: None,
		message: format!("Invalid base58 string: '{}'", value),
		column: None,
		fragment,
		label: Some("Invalid base58 encoding found".to_string()),
		help: Some("Base58 strings should only contain 1-9, A-H, J-N, P-Z, a-k, m-z characters".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

/// Invalid UTF-8 sequence in BLOB
pub fn invalid_utf8_sequence(error: Utf8Error) -> Diagnostic {
	Diagnostic {
		code: "BLOB_004".to_string(),
		statement: None,
		message: format!("Invalid UTF-8 sequence in BLOB: {}", error),
		column: None,
		fragment: Fragment::internal(error.to_string()),
		label: Some("BLOB contains invalid UTF-8 bytes".to_string()),
		help: Some("Use to_utf8_lossy() if you want to replace invalid sequences with replacement characters"
			.to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}
