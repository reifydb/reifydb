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
		self.value()
	}
}

impl<'a> Fragment for BorrowedFragment<'a> {
	type SubFragment = BorrowedFragment<'a>;

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

	fn line(&self) -> StatementLine {
		match self {
			BorrowedFragment::Statement {
				line,
				..
			} => *line,
			_ => StatementLine(1),
		}
	}

	fn column(&self) -> StatementColumn {
		match self {
			BorrowedFragment::Statement {
				column,
				..
			} => *column,
			_ => StatementColumn(0),
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

	fn sub_fragment(&self, offset: usize, length: usize) -> OwnedFragment {
		let text = self.value();
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

// Implement Fragment for &BorrowedFragment as well
impl<'a> Fragment for &BorrowedFragment<'a> {
	type SubFragment = BorrowedFragment<'a>;

	fn value(&self) -> &str {
		(*self).value()
	}

	fn line(&self) -> StatementLine {
		(*self).line()
	}

	fn column(&self) -> StatementColumn {
		(*self).column()
	}

	fn into_owned(self) -> OwnedFragment {
		(*self).into_owned()
	}

	fn sub_fragment(&self, offset: usize, length: usize) -> OwnedFragment {
		(*self).sub_fragment(offset, length)
	}
}
