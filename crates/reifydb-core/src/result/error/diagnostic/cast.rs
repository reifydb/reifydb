// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	Type,
	interface::fragment::{Fragment, IntoFragment},
	result::error::diagnostic::Diagnostic,
};

pub fn unsupported_cast(
	fragment: impl IntoFragment,
	from_type: Type,
	to_type: Type,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
	let label = Some(format!(
		"cannot cast {} of type {} to {}",
		fragment.value(),
		from_type,
		to_type
	));
	Diagnostic {
        code: "CAST_001".to_string(),
        statement: None,
        message: format!("unsupported cast from {} to {}", from_type, to_type),
        fragment,
        label,
        help: Some("ensure the source and target types are compatible for casting".to_string()),
        notes: vec![
            "supported casts include: numeric to numeric, string to temporal, boolean to numeric"
                .to_string(),
        ],
        column: None,
        cause: None,
    }
}

pub fn invalid_number(
	fragment: impl IntoFragment,
	target: Type,
	cause: Diagnostic,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
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

pub fn invalid_temporal(
	fragment: impl IntoFragment,
	target: Type,
	cause: Diagnostic,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
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

pub fn invalid_boolean(
	fragment: impl IntoFragment,
	cause: Diagnostic,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
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

pub fn invalid_uuid(
	fragment: impl IntoFragment,
	target: Type,
	cause: Diagnostic,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
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

pub fn invalid_blob_to_utf8(
	fragment: impl IntoFragment,
	cause: Diagnostic,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
	let label = Some("failed to cast BLOB to UTF8".to_string());
	Diagnostic {
        code: "CAST_006".to_string(),
        statement: None,
        message: "failed to cast BLOB to UTF8".to_string(),
        fragment,
        label,
        help: Some("BLOB contains invalid UTF-8 bytes. Consider using to_utf8_lossy() function instead".to_string()),
        notes: vec![],
        column: None,
        cause: Some(Box::from(cause)),
    }
}
