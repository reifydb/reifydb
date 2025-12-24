// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use crate::{Fragment, Type, error::diagnostic::Diagnostic};

pub fn unsupported_cast(fragment: Fragment, from_type: Type, to_type: Type) -> Diagnostic {
	let label = Some(format!("cannot cast {} of type {} to {}", fragment.text(), from_type, to_type));
	Diagnostic {
		code: "CAST_001".to_string(),
		statement: None,
		message: format!("unsupported cast from {} to {}", from_type, to_type),
		fragment,
		label,
		help: Some("ensure the source and target types are compatible for casting".to_string()),
		notes: vec!["supported casts include: numeric to numeric, string to temporal, boolean to numeric"
			.to_string()],
		column: None,
		cause: None,
	}
}

pub fn invalid_number(fragment: Fragment, target: Type, cause: Diagnostic) -> Diagnostic {
	let label = Some(format!("failed to cast to {}", target));
	Diagnostic {
		code: "CAST_002".to_string(),
		statement: None,
		message: format!("failed to cast to {}", target),
		fragment,
		label,
		help: None,
		notes: vec![],
		column: None,
		cause: Some(Box::from(cause)),
	}
}

pub fn invalid_temporal(fragment: Fragment, target: Type, cause: Diagnostic) -> Diagnostic {
	let label = Some(format!("failed to cast to {}", target));
	Diagnostic {
		code: "CAST_003".to_string(),
		statement: None,
		message: format!("failed to cast to {}", target),
		fragment,
		label,
		help: None,
		notes: vec![],
		column: None,
		cause: Some(Box::from(cause)),
	}
}

pub fn invalid_boolean(fragment: Fragment, cause: Diagnostic) -> Diagnostic {
	let label = Some("failed to cast to bool".to_string());
	Diagnostic {
		code: "CAST_004".to_string(),
		statement: None,
		message: "failed to cast to bool".to_string(),
		fragment,
		label,
		help: None,
		notes: vec![],
		column: None,
		cause: Some(Box::from(cause)),
	}
}

pub fn invalid_uuid(fragment: Fragment, target: Type, cause: Diagnostic) -> Diagnostic {
	let label = Some(format!("failed to cast to {}", target));
	Diagnostic {
		code: "CAST_005".to_string(),
		statement: None,
		message: format!("failed to cast to {}", target),
		fragment,
		label,
		help: None,
		notes: vec![],
		column: None,
		cause: Some(Box::from(cause)),
	}
}

pub fn invalid_blob_to_utf8(fragment: Fragment, cause: Diagnostic) -> Diagnostic {
	let label = Some("failed to cast BLOB to UTF8".to_string());
	Diagnostic {
		code: "CAST_006".to_string(),
		statement: None,
		message: "failed to cast BLOB to UTF8".to_string(),
		fragment,
		label,
		help: Some("BLOB contains invalid UTF-8 bytes. Consider using to_utf8_lossy() function instead"
			.to_string()),
		notes: vec![],
		column: None,
		cause: Some(Box::from(cause)),
	}
}
