// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub mod borrowed;
pub mod owned;

pub use borrowed::BorrowedFragment;
pub use owned::OwnedFragment;

// Re-export position types (will be renamed later)
pub use crate::result::error::diagnostic::origin::{
	SpanColumn as StatementColumn, SpanLine as StatementLine,
};

/// Macro to create Fragment with automatic location capture
#[macro_export]
macro_rules! fragment {
	// Statement fragment from span with location tracking
	(statement: $span:expr) => {{
		let span = $crate::IntoOwnedSpan::into_span($span);
		$crate::interface::fragment::OwnedFragment::Statement {
			text: span.fragment,
			line: $crate::interface::fragment::StatementLine(
				span.line.0,
			),
			column: $crate::interface::fragment::StatementColumn(
				span.column.0,
			),
		}
	}};

	// Internal fragment with location tracking
	(internal: $text:expr) => {
		$crate::interface::fragment::OwnedFragment::Internal {
			text: $text.to_string(),
		}
	};

	// None variant
	(none) => {
		$crate::interface::fragment::OwnedFragment::None
	};
}

/// Core trait for fragment types
pub trait Fragment: Clone {
	/// Get the text value of the fragment
	fn value(&self) -> &str;

	/// Convert to owned variant
	fn into_owned(self) -> OwnedFragment
	where
		Self: Sized;

	/// Get position information for Statement fragments
	fn position(&self) -> Option<(u32, u32)>;
}

/// Trait for types that can be converted into a Fragment
pub trait IntoFragment {
	fn into_fragment(self) -> OwnedFragment;
}

// Blanket implementation for any Fragment type
impl<T: Fragment> IntoFragment for T {
	fn into_fragment(self) -> OwnedFragment {
		self.into_owned()
	}
}

// Conversions from old Span types for backward compatibility
use crate::{BorrowedSpan, OwnedSpan};

impl IntoFragment for OwnedSpan {
	fn into_fragment(self) -> OwnedFragment {
		fragment!(statement: self)
	}
}

impl IntoFragment for &OwnedSpan {
	fn into_fragment(self) -> OwnedFragment {
		fragment!(statement: self.clone())
	}
}

impl<'a> IntoFragment for BorrowedSpan<'a> {
	fn into_fragment(self) -> OwnedFragment {
		fragment!(statement: self)
	}
}

impl<'a> IntoFragment for &BorrowedSpan<'a> {
	fn into_fragment(self) -> OwnedFragment {
		fragment!(statement: *self)
	}
}

// Implementation for Option<OwnedSpan>
impl IntoFragment for Option<OwnedSpan> {
	fn into_fragment(self) -> OwnedFragment {
		match self {
			Some(span) => span.into_fragment(),
			None => OwnedFragment::None,
		}
	}
}

// Also provide From implementations for convenience
impl From<Option<OwnedSpan>> for OwnedFragment {
	fn from(span_opt: Option<OwnedSpan>) -> Self {
		span_opt.into_fragment()
	}
}
