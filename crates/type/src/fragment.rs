// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::{
	cmp::Ordering,
	fmt::{Display, Formatter},
	ops::Deref,
};

use serde::{Deserialize, Serialize};

// Position types for fragments
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StatementColumn(pub u32);

impl Deref for StatementColumn {
	type Target = u32;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<i32> for StatementColumn {
	fn eq(&self, other: &i32) -> bool {
		self.0 == *other as u32
	}
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StatementLine(pub u32);

impl Deref for StatementLine {
	type Target = u32;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<i32> for StatementLine {
	fn eq(&self, other: &i32) -> bool {
		self.0 == *other as u32
	}
}

/// Fragment - owns all its data
#[derive(Debug, Clone, PartialEq, Hash, Serialize, Deserialize)]
pub enum Fragment {
	/// No fragment information available
	None,

	/// Fragment from a RQL statement with position information
	Statement {
		text: String,
		line: StatementLine,
		column: StatementColumn,
	},

	/// Fragment from internal/runtime code
	Internal {
		text: String,
	},
}

impl Fragment {
	/// Get the text value of the fragment
	pub fn text(&self) -> &str {
		match self {
			Fragment::None => "",
			Fragment::Statement {
				text,
				..
			}
			| Fragment::Internal {
				text,
				..
			} => text,
		}
	}

	/// Get line position
	pub fn line(&self) -> StatementLine {
		match self {
			Fragment::Statement {
				line,
				..
			} => *line,
			_ => StatementLine(1),
		}
	}

	/// Get column position
	pub fn column(&self) -> StatementColumn {
		match self {
			Fragment::Statement {
				column,
				..
			} => *column,
			_ => StatementColumn(0),
		}
	}

	/// Convert to owned variant (identity function now)
	pub fn into_owned(self) -> Fragment {
		self
	}

	/// Get a sub-fragment starting at the given offset with the given
	/// length
	pub fn sub_fragment(&self, offset: usize, length: usize) -> Fragment {
		let text = self.text();
		let end = std::cmp::min(offset + length, text.len());
		let sub_text = if offset < text.len() {
			&text[offset..end]
		} else {
			""
		};

		match self {
			Fragment::None => Fragment::None,
			Fragment::Statement {
				line,
				column,
				..
			} => Fragment::Statement {
				text: sub_text.to_string(),
				line: *line,
				column: StatementColumn(column.0 + offset as u32),
			},
			Fragment::Internal {
				..
			} => Fragment::Internal {
				text: sub_text.to_string(),
			},
		}
	}
}

impl Fragment {
	/// Create an internal fragment - useful for creating fragments from
	/// substrings
	pub fn internal(text: impl Into<String>) -> Self {
		Fragment::Internal {
			text: text.into(),
		}
	}

	/// Create a testing fragment - returns a Statement fragment for test
	/// purposes
	pub fn testing(text: impl Into<String>) -> Self {
		Fragment::Statement {
			text: text.into(),
			line: StatementLine(1),
			column: StatementColumn(0),
		}
	}

	/// Create an empty testing fragment
	pub fn testing_empty() -> Self {
		Self::testing("")
	}

	/// Merge multiple fragments (in any order) into one encompassing
	/// fragment
	pub fn merge_all(fragments: impl IntoIterator<Item = Fragment>) -> Fragment {
		let mut fragments: Vec<Fragment> = fragments.into_iter().collect();
		assert!(!fragments.is_empty());

		fragments.sort();

		let first = fragments.first().unwrap();

		let mut text = String::with_capacity(fragments.iter().map(|f| f.text().len()).sum());
		for fragment in &fragments {
			text.push_str(fragment.text());
		}

		match first {
			Fragment::None => Fragment::None,
			Fragment::Statement {
				line,
				column,
				..
			} => Fragment::Statement {
				text,
				line: *line,
				column: *column,
			},
			Fragment::Internal {
				..
			} => Fragment::Internal {
				text,
			},
		}
	}

	/// Compatibility: expose fragment field for Fragment compatibility
	pub fn fragment(&self) -> &str {
		self.text()
	}
}

impl Default for Fragment {
	fn default() -> Self {
		Fragment::None
	}
}

impl AsRef<str> for Fragment {
	fn as_ref(&self) -> &str {
		self.text()
	}
}

impl Display for Fragment {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		Display::fmt(self.text(), f)
	}
}

impl PartialOrd for Fragment {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Fragment {
	fn cmp(&self, other: &Self) -> Ordering {
		self.column().cmp(&other.column()).then(self.line().cmp(&other.line()))
	}
}

impl Eq for Fragment {}

// Convenience From implementations
impl From<String> for Fragment {
	fn from(s: String) -> Self {
		Fragment::Internal {
			text: s,
		}
	}
}

impl From<&str> for Fragment {
	fn from(s: &str) -> Self {
		Fragment::Internal {
			text: s.to_string(),
		}
	}
}

impl Fragment {
	/// Create a statement fragment with position info
	pub fn statement(text: impl Into<String>, line: u32, column: u32) -> Self {
		Fragment::Statement {
			text: text.into(),
			line: StatementLine(line),
			column: StatementColumn(column),
		}
	}

	/// Create a none fragment
	pub fn none() -> Self {
		Fragment::None
	}
}

// PartialEq implementations for Fragment with str/String
impl PartialEq<str> for Fragment {
	fn eq(&self, other: &str) -> bool {
		self.text() == other
	}
}

impl PartialEq<&str> for Fragment {
	fn eq(&self, other: &&str) -> bool {
		self.text() == *other
	}
}

impl PartialEq<String> for Fragment {
	fn eq(&self, other: &String) -> bool {
		self.text() == other.as_str()
	}
}

impl PartialEq<String> for &Fragment {
	fn eq(&self, other: &String) -> bool {
		self.text() == other.as_str()
	}
}

/// Trait for types that can lazily provide a Fragment
pub trait LazyFragment {
	fn fragment(&self) -> Fragment;
}

impl<F> LazyFragment for F
where
	F: Fn() -> Fragment,
{
	fn fragment(&self) -> Fragment {
		self()
	}
}

impl LazyFragment for &Fragment {
	fn fragment(&self) -> Fragment {
		(*self).clone()
	}
}

impl LazyFragment for Fragment {
	fn fragment(&self) -> Fragment {
		self.clone()
	}
}
