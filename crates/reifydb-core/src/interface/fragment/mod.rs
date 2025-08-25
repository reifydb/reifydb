// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub mod borrowed;
pub mod owned;

use std::ops::Deref;

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

/// Core enum for fragment types
#[derive(Debug, Clone)]
pub enum Fragment<'a> {
	Owned(OwnedFragment),
	Borrowed(BorrowedFragment<'a>),
	None,
}

impl<'a> Fragment<'a> {
	/// Get the text value of the fragment
	pub fn value(&self) -> &str {
		match self {
			Fragment::Owned(f) => f.value(),
			Fragment::Borrowed(f) => f.value(),
			Fragment::None => "",
		}
	}

	/// Alias for value() for compatibility
	pub fn fragment(&self) -> &str {
		self.value()
	}

	/// Get line position
	pub fn line(&self) -> StatementLine {
		match self {
			Fragment::Owned(f) => f.line(),
			Fragment::Borrowed(f) => f.line(),
			Fragment::None => StatementLine(1),
		}
	}

	/// Get column position
	pub fn column(&self) -> StatementColumn {
		match self {
			Fragment::Owned(f) => f.column(),
			Fragment::Borrowed(f) => f.column(),
			Fragment::None => StatementColumn(0),
		}
	}

	/// Convert to owned variant
	pub fn into_owned(self) -> OwnedFragment {
		match self {
			Fragment::Owned(f) => f,
			Fragment::Borrowed(f) => f.into_owned(),
			Fragment::None => OwnedFragment::None,
		}
	}

	/// Convert to owned variant (alias for compatibility)
	pub fn to_owned(self) -> OwnedFragment {
		self.into_owned()
	}

	/// Get a sub-fragment starting at the given offset with the given
	/// length
	pub fn sub_fragment(
		&self,
		offset: usize,
		length: usize,
	) -> OwnedFragment {
		match self {
			Fragment::Owned(f) => f.sub_fragment(offset, length),
			Fragment::Borrowed(f) => f.sub_fragment(offset, length),
			Fragment::None => OwnedFragment::None,
		}
	}
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

// Implementation for Fragment enum
impl<'a> IntoFragment for Fragment<'a> {
	fn into_fragment(self) -> OwnedFragment {
		self.into_owned()
	}
}

// Implementation for OwnedFragment
impl IntoFragment for OwnedFragment {
	fn into_fragment(self) -> OwnedFragment {
		self
	}
}

// Implementation for BorrowedFragment
impl<'a> IntoFragment for BorrowedFragment<'a> {
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

impl IntoFragment for &str {
	fn into_fragment(self) -> OwnedFragment {
		OwnedFragment::internal(self)
	}
}

impl IntoFragment for &String {
	fn into_fragment(self) -> OwnedFragment {
		OwnedFragment::internal(self)
	}
}
