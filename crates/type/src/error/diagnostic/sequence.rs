// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use crate::{
	Fragment, Type,
	error::diagnostic::{Diagnostic, util::value_max},
};

pub fn sequence_exhausted(value: Type) -> Diagnostic {
	Diagnostic {
		code: "SEQUENCE_001".to_string(),
		statement: None,
		message: format!("sequence generator of type `{}` is exhausted", value),
		fragment: Fragment::None,
		label: Some("no more values can be generated".to_string()),
		help: Some(format!("maximum value for `{}` is `{}`", value, value_max(value))),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn can_not_alter_not_auto_increment(fragment: Fragment) -> Diagnostic {
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
		operator_chain: None,
	}
}
