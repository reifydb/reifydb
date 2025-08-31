// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	OwnedFragment, Type,
	interface::fragment::IntoFragment,
	result::error::diagnostic::{Diagnostic, util::value_max},
};

pub fn sequence_exhausted(value: Type) -> Diagnostic {
	Diagnostic {
		code: "SEQUENCE_001".to_string(),
		statement: None,
		message: format!(
			"sequence generator of type `{}` is exhausted",
			value
		),
		fragment: OwnedFragment::None,
		label: Some("no more values can be generated".to_string()),
		help: Some(format!(
			"maximum value for `{}` is `{}`",
			value,
			value_max(value)
		)),
		column: None,
		notes: vec![],
		cause: None,
	}
}

pub fn can_not_alter_not_auto_increment<'a>(
	fragment: impl IntoFragment<'a>,
) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	Diagnostic {
        code: "SEQUENCE_002".to_string(),
        statement: None,
        message: format!(
            "cannot alter sequence for column `{}` which does not have AUTO INCREMENT",
            fragment.text()
        ),
        fragment,
        label: Some("column does not have AUTO INCREMENT".to_string()),
        help: Some("only columns with AUTO INCREMENT can have their sequences altered".to_string()),
        column: None,
        notes: vec![],
        cause: None,
    }
}

pub fn transaction_sequence_exhausted() -> Diagnostic {
	Diagnostic {
        code: "SEQUENCE_003".to_string(),
        statement: None,
        message: "transaction sequence number exhausted".to_string(),
        fragment: OwnedFragment::None,
        label: Some("no more CDC sequence numbers available for this version".to_string()),
        help: Some("transaction has reached the maximum of 65535 CDC events (sequences 1-65535)".to_string()),
        column: None,
        notes: vec![],
        cause: None,
    }
}
