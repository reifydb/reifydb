// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub use borrowed::BorrowedSpan;
pub use owned::OwnedSpan;
use serde::{Deserialize, Serialize};

mod borrowed;
mod owned;

// Type aliases for gradual migration
pub type OwnedStatementOrigin = OwnedSpan;
pub type BorrowedStatementOrigin<'a> = BorrowedSpan<'a>;

/// Represents the origin of a diagnostic error
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DiagnosticOrigin {
	/// No origin information available
	None,

	/// From RQL statement with position
	Statement {
		line: SpanLine,
		column: SpanColumn,
		fragment: String,
		// Location where the error was created
		module: String,
		file: String,
		location_line: u32,
	},

	/// Internal error with location capture
	Internal {
		fragment: String,
		// Location where the error was created
		module: String,
		file: String,
		line: u32,
	},
}

pub enum Fragment {
	Empty {
		code: Code,
	},
	Statement {
		value: String,
		line: SpanLine, // Refactor to StatementLine
		column: SpanColumn, // Refactor to StatementColumn
		code: Code,
	},
	Internal {
		value: String,
		code: Code,
	},
}

pub struct Code {
	module: String,
	file: String,
	line: u32,
}

impl DiagnosticOrigin {
	/// Get the fragment regardless of origin type
	pub fn fragment(&self) -> Option<&str> {
		match self {
			DiagnosticOrigin::None => None,
			DiagnosticOrigin::Statement {
				fragment,
				..
			}
			| DiagnosticOrigin::Internal {
				fragment,
				..
			} => Some(fragment),
		}
	}

	/// Get location info (module, file, line) where the diagnostic was created
	pub fn location_info(&self) -> Option<(&str, &str, u32)> {
		match self {
			DiagnosticOrigin::None => None,
			DiagnosticOrigin::Statement {
				module,
				file,
				location_line,
				..
			} => Some((
				module.as_str(),
				file.as_str(),
				*location_line,
			)),
			DiagnosticOrigin::Internal {
				module,
				file,
				line,
				..
			} => Some((module.as_str(), file.as_str(), *line)),
		}
	}
}

/// Macro to create DiagnosticOrigin with automatic location capture
#[macro_export]
macro_rules! diagnostic_origin {
	// Statement origin from span with location tracking
	(statement: $span:expr) => {{
		let span = $crate::IntoOwnedSpan::into_span($span);
		$crate::DiagnosticOrigin::Statement {
			line: span.line,
			column: span.column,
			fragment: span.fragment,
			module: module_path!().to_string(),
			file: file!().to_string(),
			location_line: line!(),
		}
	}};

	// Internal error with location tracking
	(internal: $fragment:expr) => {
		$crate::DiagnosticOrigin::Internal {
			fragment: $fragment.to_string(),
			module: module_path!().to_string(),
			file: file!().to_string(),
			line: line!(),
		}
	};
}

/// Trait for types that can be converted into a DiagnosticOrigin
pub trait IntoDiagnosticOrigin {
	fn into_origin(self) -> DiagnosticOrigin;
}

// Implementation for DiagnosticOrigin itself (identity)
impl IntoDiagnosticOrigin for DiagnosticOrigin {
	fn into_origin(self) -> DiagnosticOrigin {
		self
	}
}

// Implementation for OwnedSpan - converts to Statement origin
impl IntoDiagnosticOrigin for OwnedSpan {
	fn into_origin(self) -> DiagnosticOrigin {
		diagnostic_origin!(statement: self)
	}
}

// Implementation for reference to OwnedSpan
impl IntoDiagnosticOrigin for &OwnedSpan {
	fn into_origin(self) -> DiagnosticOrigin {
		diagnostic_origin!(statement: self.clone())
	}
}

// Implementation for BorrowedSpan - converts to Statement origin
impl<'a> IntoDiagnosticOrigin for BorrowedSpan<'a> {
	fn into_origin(self) -> DiagnosticOrigin {
		diagnostic_origin!(statement: self)
	}
}

// Implementation for reference to BorrowedSpan
impl<'a> IntoDiagnosticOrigin for &BorrowedSpan<'a> {
	fn into_origin(self) -> DiagnosticOrigin {
		diagnostic_origin!(statement: self.clone())
	}
}

// Implementation for Option<OwnedSpan> - None becomes DiagnosticOrigin::None
impl From<Option<OwnedSpan>> for DiagnosticOrigin {
	fn from(span_opt: Option<OwnedSpan>) -> Self {
		match span_opt {
			Some(span) => span.into_origin(),
			None => DiagnosticOrigin::None,
		}
	}
}

// Also implement IntoDiagnosticOrigin for Option<OwnedSpan>
impl IntoDiagnosticOrigin for Option<OwnedSpan> {
	fn into_origin(self) -> DiagnosticOrigin {
		DiagnosticOrigin::from(self)
	}
}

// Trait for types that can provide span information for parsing
pub trait Span: Clone {
	type SubSpan: Span + IntoOwnedSpan + crate::interface::fragment::IntoFragment;

	fn fragment(&self) -> &str;
	fn line(&self) -> SpanLine;
	fn column(&self) -> SpanColumn;

	/// Get the fragment with leading and trailing whitespace trimmed
	fn trimmed_fragment(&self) -> &str {
		self.fragment().trim()
	}

	/// Split this span by delimiter, returning a vector of spans for each
	/// part. For OwnedSpan, returns Vec<OwnedSpan>. For BorrowedSpan,
	/// returns Vec<BorrowedSpan>.
	fn split(&self, delimiter: char) -> Vec<Self::SubSpan>;

	/// Convert to owned version
	fn to_owned(self) -> OwnedSpan
	where
		Self: Sized;

	/// Get a sub-span starting at the given offset with the given length.
	/// For OwnedSpan, returns OwnedSpan. For BorrowedSpan, returns
	/// BorrowedSpan.
	fn sub_span(&self, offset: usize, length: usize) -> Self::SubSpan;
}

/// Trait to provide a `OwnedSpan` either directly or lazily (via closure).
pub trait IntoOwnedSpan {
	fn into_span(self) -> OwnedSpan;
}

impl IntoOwnedSpan for OwnedSpan {
	fn into_span(self) -> OwnedSpan {
		self
	}
}

impl IntoOwnedSpan for &OwnedSpan {
	fn into_span(self) -> OwnedSpan {
		self.clone()
	}
}

impl<F> IntoOwnedSpan for F
where
	F: Fn() -> OwnedSpan,
{
	fn into_span(self) -> OwnedSpan {
		self()
	}
}

impl<'a> IntoOwnedSpan for BorrowedSpan<'a> {
	fn into_span(self) -> OwnedSpan {
		OwnedSpan {
			column: self.column,
			line: self.line,
			fragment: self.fragment.to_string(),
		}
	}
}

impl<'a> IntoOwnedSpan for &BorrowedSpan<'a> {
	fn into_span(self) -> OwnedSpan {
		OwnedSpan {
			column: self.column,
			line: self.line,
			fragment: self.fragment.to_string(),
		}
	}
}

#[repr(transparent)]
#[derive(
	Debug,
	Clone,
	Copy,
	PartialEq,
	Eq,
	PartialOrd,
	Ord,
	Serialize,
	Deserialize,
)]
pub struct SpanColumn(pub u32);

impl PartialEq<i32> for SpanColumn {
	fn eq(&self, other: &i32) -> bool {
		self.0 == *other as u32
	}
}

#[repr(transparent)]
#[derive(
	Debug,
	Clone,
	Copy,
	PartialEq,
	Eq,
	PartialOrd,
	Ord,
	Serialize,
	Deserialize,
)]
pub struct SpanLine(pub u32);

impl PartialEq<i32> for SpanLine {
	fn eq(&self, other: &i32) -> bool {
		self.0 == *other as u32
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_option_owned_span_to_diagnostic_origin() {
		// Test None case
		let none_span: Option<OwnedSpan> = None;
		let origin = DiagnosticOrigin::from(none_span);
		assert_eq!(origin, DiagnosticOrigin::None);

		// Test Some case
		let some_span = Some(OwnedSpan {
			line: SpanLine(10),
			column: SpanColumn(5),
			fragment: "test fragment".to_string(),
		});
		let origin = DiagnosticOrigin::from(some_span);

		// Verify it's a Statement variant with correct data
		match origin {
			DiagnosticOrigin::Statement {
				line,
				column,
				fragment,
				..
			} => {
				assert_eq!(line, SpanLine(10));
				assert_eq!(column, SpanColumn(5));
				assert_eq!(fragment, "test fragment");
			}
			_ => panic!("Expected Statement variant"),
		}
	}

	#[test]
	fn test_option_owned_span_into_diagnostic_origin() {
		// Test using IntoDiagnosticOrigin trait
		let none_span: Option<OwnedSpan> = None;
		let origin = none_span.into_origin();
		assert_eq!(origin, DiagnosticOrigin::None);

		let some_span = Some(OwnedSpan {
			line: SpanLine(20),
			column: SpanColumn(15),
			fragment: "another test".to_string(),
		});
		let origin = some_span.into_origin();

		match origin {
			DiagnosticOrigin::Statement {
				line,
				column,
				fragment,
				..
			} => {
				assert_eq!(line, SpanLine(20));
				assert_eq!(column, SpanColumn(15));
				assert_eq!(fragment, "another test");
			}
			_ => panic!("Expected Statement variant"),
		}
	}
}
