// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use crate::{Fragment, error::diagnostic::Diagnostic};

pub fn column_not_found(fragment: Fragment) -> Diagnostic {
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
		operator_chain: None,
	}
}

pub fn extend_duplicate_column(column_name: &str) -> Diagnostic {
	Diagnostic {
		code: "EXTEND_002".to_string(),
		statement: None,
		message: format!("Cannot extend with duplicate column name '{}'", column_name),
		fragment: crate::fragment::Fragment::None,
		label: Some("column already exists in the current frame".to_string()),
		help: Some("Use a different column name or remove the existing column first".to_string()),
		column: None,
		notes: vec![
			"EXTEND operation cannot add columns that already exist in the frame".to_string(),
			"Each column name must be unique within the result frame".to_string(),
			"Consider using MAP if you want to replace existing columns".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

pub fn unsupported_source_qualification(fragment: Fragment, name: &str) -> Diagnostic {
	Diagnostic {
		code: "QUERY_002".to_string(),
		statement: None,
		message: format!("Source qualification '{}' is not supported in RQL expressions", name),
		fragment,
		label: Some("source qualification is only allowed for join aliases in ON clauses".to_string()),
		help: Some("Remove the qualification or use it only with a join alias in the ON clause".to_string()),
		column: None,
		notes: vec![
			"RQL uses a dataframe-centered approach where each operation produces a new dataframe"
				.to_string(),
			"Source qualifications are not needed as columns are unambiguous in the dataframe".to_string(),
			"Only join aliases can be used for qualification within ON clauses to disambiguate columns"
				.to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}
