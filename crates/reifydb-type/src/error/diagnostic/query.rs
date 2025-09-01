// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use crate::error::diagnostic::Diagnostic;
use crate::fragment::IntoFragment;

pub fn column_not_found<'a>(fragment: impl IntoFragment<'a>) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	Diagnostic {
        code: "QUERY_001".to_string(),
        statement: None,
        message: "column not found".to_string(),
        fragment,
        label: Some("this column does not exist in the current context".to_string()),
        help: Some("check for typos or ensure the column is defined in the input".to_string()),
        column: None,
        notes: vec![],
        cause: None,
    }
}

pub fn extend_duplicate_column(column_name: &str) -> Diagnostic {
	Diagnostic {
		code: "EXTEND_002".to_string(),
		statement: None,
		message: format!("Cannot extend with duplicate column name '{}'", column_name),
		fragment: crate::fragment::OwnedFragment::None,
		label: Some("column already exists in the current frame".to_string()),
		help: Some("Use a different column name or remove the existing column first".to_string()),
		column: None,
		notes: vec![
			"EXTEND operation cannot add columns that already exist in the frame".to_string(),
			"Each column name must be unique within the result frame".to_string(),
			"Consider using MAP if you want to replace existing columns".to_string(),
		],
		cause: None,
	}
}
