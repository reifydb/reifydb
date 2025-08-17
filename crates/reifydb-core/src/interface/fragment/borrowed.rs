// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{Fragment, OwnedFragment, StatementColumn, StatementLine};

/// Borrowed fragment - zero-copy for parsing
#[derive(Debug, Copy, Clone)]
pub enum BorrowedFragment<'a> {
	/// No fragment information available
	None,

	/// Fragment from a RQL statement with position information
	Statement {
		text: &'a str,
		line: StatementLine,
		column: StatementColumn,
	},

	/// Fragment from internal/runtime code
	Internal {
		text: &'a str,
	},
}

impl<'a> Fragment for BorrowedFragment<'a> {
	fn value(&self) -> &str {
		match self {
			BorrowedFragment::None => "",
			BorrowedFragment::Statement {
				text,
				..
			}
			| BorrowedFragment::Internal {
				text,
				..
			} => text,
		}
	}

	fn into_owned(self) -> OwnedFragment {
		match self {
			BorrowedFragment::None => OwnedFragment::None,
			BorrowedFragment::Statement {
				text,
				line,
				column,
			} => OwnedFragment::Statement {
				text: text.to_string(),
				line,
				column,
			},
			BorrowedFragment::Internal {
				text,
			} => OwnedFragment::Internal {
				text: text.to_string(),
			},
		}
	}

	fn position(&self) -> Option<(u32, u32)> {
		match self {
			BorrowedFragment::Statement {
				line,
				column,
				..
			} => Some((line.0, column.0)),
			_ => None,
		}
	}
}

impl<'a> BorrowedFragment<'a> {
	/// Create a new Statement fragment
	pub fn new_statement(
		text: &'a str,
		line: StatementLine,
		column: StatementColumn,
	) -> Self {
		BorrowedFragment::Statement {
			text,
			line,
			column,
		}
	}

	/// Create a new Internal fragment
	pub fn new_internal(text: &'a str) -> Self {
		BorrowedFragment::Internal {
			text,
		}
	}
}
