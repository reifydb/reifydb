// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use super::{OwnedFragment, StatementColumn, StatementLine};

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

impl<'a> BorrowedFragment<'a> {
	/// Create a new borrowed fragment with default position
	pub fn new(text: &'a str) -> Self {
		Self::Statement {
			text,
			line: StatementLine(1),
			column: StatementColumn(0),
		}
	}

	/// Create a new Internal fragment
	pub fn new_internal(text: &'a str) -> Self {
		BorrowedFragment::Internal {
			text,
		}
	}

	/// Compatibility: expose fragment field
	pub fn fragment(&self) -> &str {
		self.text()
	}
}

impl<'a> BorrowedFragment<'a> {
	/// Get the text value of the fragment
	pub fn text(&self) -> &str {
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

	/// Get line position
	pub fn line(&self) -> StatementLine {
		match self {
			BorrowedFragment::Statement {
				line,
				..
			} => *line,
			_ => StatementLine(1),
		}
	}

	/// Get column position
	pub fn column(&self) -> StatementColumn {
		match self {
			BorrowedFragment::Statement {
				column,
				..
			} => *column,
			_ => StatementColumn(0),
		}
	}

	/// Convert to owned variant
	pub fn into_owned(self) -> OwnedFragment {
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

	/// Get a sub-fragment starting at the given offset with the given
	/// length
	pub fn sub_fragment(
		&self,
		offset: usize,
		length: usize,
	) -> OwnedFragment {
		let text = self.text();
		let end = std::cmp::min(offset + length, text.len());
		let sub_text = if offset < text.len() {
			&text[offset..end]
		} else {
			""
		};

		match self {
			BorrowedFragment::None => OwnedFragment::None,
			BorrowedFragment::Statement {
				line,
				column,
				..
			} => OwnedFragment::Statement {
				text: sub_text.to_string(),
				line: *line,
				column: StatementColumn(
					column.0 + offset as u32,
				),
			},
			BorrowedFragment::Internal {
				..
			} => OwnedFragment::Internal {
				text: sub_text.to_string(),
			},
		}
	}
}

// Implement methods for &BorrowedFragment as well
impl<'a> BorrowedFragment<'a> {
	/// Clone and convert to owned
	pub fn to_owned(&self) -> OwnedFragment {
		(*self).into_owned()
	}
}
