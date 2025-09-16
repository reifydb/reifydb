// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use super::Diagnostic;
use crate::OwnedFragment;

pub fn utf8_exceeds_max_bytes(fragment: OwnedFragment, actual: usize, max: usize) -> Diagnostic {
	Diagnostic {
		code: "CONSTRAINT_001".to_string(),
		statement: None,
		message: format!("UTF8 value exceeds maximum byte length: {} bytes (max: {} bytes)", actual, max),
		column: None,
		fragment,
		label: Some("constraint violation".to_string()),
		help: Some(format!(
			"The UTF8 field is constrained to a maximum of {} bytes. Consider shortening the text or increasing the constraint.",
			max
		)),
		notes: vec![],
		cause: None,
	}
}

pub fn blob_exceeds_max_bytes(fragment: OwnedFragment, actual: usize, max: usize) -> Diagnostic {
	Diagnostic {
		code: "CONSTRAINT_002".to_string(),
		statement: None,
		message: format!("BLOB value exceeds maximum byte length: {} bytes (max: {} bytes)", actual, max),
		column: None,
		fragment,
		label: Some("constraint violation".to_string()),
		help: Some(format!(
			"The BLOB field is constrained to a maximum of {} bytes. Consider reducing the data size or increasing the constraint.",
			max
		)),
		notes: vec![],
		cause: None,
	}
}

pub fn int_exceeds_max_bytes(fragment: OwnedFragment, actual: usize, max: usize) -> Diagnostic {
	Diagnostic {
		code: "CONSTRAINT_003".to_string(),
		statement: None,
		message: format!("INT value exceeds maximum byte length: {} bytes (max: {} bytes)", actual, max),
		column: None,
		fragment,
		label: Some("constraint violation".to_string()),
		help: Some(format!(
			"The INT field is constrained to a maximum of {} bytes. Consider using a smaller value or increasing the constraint.",
			max
		)),
		notes: vec![],
		cause: None,
	}
}

pub fn uint_exceeds_max_bytes(fragment: OwnedFragment, actual: usize, max: usize) -> Diagnostic {
	Diagnostic {
		code: "CONSTRAINT_004".to_string(),
		statement: None,
		message: format!("UINT value exceeds maximum byte length: {} bytes (max: {} bytes)", actual, max),
		column: None,
		fragment,
		label: Some("constraint violation".to_string()),
		help: Some(format!(
			"The UINT field is constrained to a maximum of {} bytes. Consider using a smaller value or increasing the constraint.",
			max
		)),
		notes: vec![],
		cause: None,
	}
}

pub fn decimal_exceeds_precision(fragment: OwnedFragment, actual: u8, max: u8) -> Diagnostic {
	Diagnostic {
		code: "CONSTRAINT_005".to_string(),
		statement: None,
		message: format!("DECIMAL value exceeds maximum precision: {} digits (max: {} digits)", actual, max),
		column: None,
		fragment,
		label: Some("constraint violation".to_string()),
		help: Some(format!(
			"The DECIMAL field is constrained to a maximum precision of {} digits. Consider using a smaller number or increasing the precision constraint.",
			max
		)),
		notes: vec![],
		cause: None,
	}
}

pub fn decimal_exceeds_scale(fragment: OwnedFragment, actual: u8, max: u8) -> Diagnostic {
	Diagnostic {
		code: "CONSTRAINT_006".to_string(),
		statement: None,
		message: format!(
			"DECIMAL value exceeds maximum scale: {} decimal places (max: {} decimal places)",
			actual, max
		),
		column: None,
		fragment,
		label: Some("constraint violation".to_string()),
		help: Some(format!(
			"The DECIMAL field is constrained to a maximum of {} decimal places. Consider rounding the value or increasing the scale constraint.",
			max
		)),
		notes: vec![],
		cause: None,
	}
}
