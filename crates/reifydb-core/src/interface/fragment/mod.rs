// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub mod borrowed;
pub mod owned;

pub use borrowed::BorrowedFragment;
pub use owned::OwnedFragment;
use serde::{Deserialize, Serialize};

// Position types for fragments
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
pub struct StatementColumn(pub u32);

impl PartialEq<i32> for StatementColumn {
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
pub struct StatementLine(pub u32);

impl PartialEq<i32> for StatementLine {
	fn eq(&self, other: &i32) -> bool {
		self.0 == *other as u32
	}
}

/// Macro to create Fragment with automatic location capture
#[macro_export]
macro_rules! fragment {
	(statement: $fragment:expr) => {{
		let fragment = $crate::interface::fragment::IntoOwnedFragment::into_fragment($fragment);
		match fragment {
			$crate::interface::fragment::OwnedFragment::Statement { text, line, column } => {
				$crate::interface::fragment::OwnedFragment::Statement {
					text,
					line,
					column,
				}
			}
			_ => fragment,
		}
	}};

	(internal: $text:expr) => {
		$crate::interface::fragment::OwnedFragment::Internal {
			text: $text.to_string(),
		}
	};

	(none) => {
		$crate::interface::fragment::OwnedFragment::None
	};
}

/// Core trait for fragment types
pub trait Fragment: Clone {
	type SubFragment: Fragment + IntoOwnedFragment + IntoFragment;

	/// Get the text value of the fragment
	fn value(&self) -> &str;

	/// Alias for value() for compatibility
	fn fragment(&self) -> &str {
		self.value()
	}

	/// Get line position
	fn line(&self) -> StatementLine;

	/// Get column position
	fn column(&self) -> StatementColumn;

	/// Convert to owned variant
	fn into_owned(self) -> OwnedFragment
	where
		Self: Sized;

	/// Convert to owned variant (alias for compatibility)
	fn to_owned(self) -> OwnedFragment
	where
		Self: Sized,
	{
		self.into_owned()
	}

	/// Get a sub-fragment starting at the given offset with the given
	/// length
	fn sub_fragment(&self, offset: usize, length: usize) -> OwnedFragment;
}

/// Trait for types that can be converted into a Fragment
pub trait IntoFragment {
	fn into_fragment(self) -> OwnedFragment;
}

/// Trait to provide an OwnedFragment either directly or lazily (via closure)
pub trait IntoOwnedFragment {
	fn into_fragment(self) -> OwnedFragment;
}

impl IntoOwnedFragment for OwnedFragment {
	fn into_fragment(self) -> OwnedFragment {
		self
	}
}

impl IntoOwnedFragment for &OwnedFragment {
	fn into_fragment(self) -> OwnedFragment {
		self.clone()
	}
}

impl<F> IntoOwnedFragment for F
where
	F: Fn() -> OwnedFragment,
{
	fn into_fragment(self) -> OwnedFragment {
		self()
	}
}

impl<'a> IntoOwnedFragment for BorrowedFragment<'a> {
	fn into_fragment(self) -> OwnedFragment {
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
}

impl<'a> IntoOwnedFragment for &BorrowedFragment<'a> {
	fn into_fragment(self) -> OwnedFragment {
		IntoOwnedFragment::into_fragment(*self)
	}
}

// Blanket implementation for any Fragment type
impl<T: Fragment> IntoFragment for T {
	fn into_fragment(self) -> OwnedFragment {
		self.into_owned()
	}
}

// Implementation for Option<OwnedFragment>
impl IntoFragment for Option<OwnedFragment> {
	fn into_fragment(self) -> OwnedFragment {
		match self {
			Some(fragment) => fragment,
			None => OwnedFragment::None,
		}
	}
}

// Also provide From implementations for convenience
impl From<Option<OwnedFragment>> for OwnedFragment {
	fn from(fragment_opt: Option<OwnedFragment>) -> Self {
		fragment_opt.into_fragment()
	}
}
