// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{error::Diagnostic, fragment::Fragment};

pub fn can_not_alter_not_auto_increment(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "SEQUENCE_002".to_string(),
		rql: None,
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
		operator_chain: None,
	}
}
