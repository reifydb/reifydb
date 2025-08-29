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

impl Fragment<'_> {
	pub fn owned_internal(text: impl Into<String>) -> Self {
		Fragment::Owned(OwnedFragment::Internal {
			text: text.into(),
		})
	}

	pub fn owned_empty() -> Self {
		Fragment::Owned(OwnedFragment::testing_empty())
	}

	pub fn borrowed_internal(text: &str) -> Fragment {
		Fragment::Borrowed(BorrowedFragment::Internal {
			text,
		})
	}

	pub fn none() -> Self {
		Self::None
	}

	pub fn testing_empty() -> Fragment<'static> {
		Fragment::Borrowed(BorrowedFragment::Internal {
			text: "",
		})
	}
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

	/// Create a borrowed view of this fragment
	pub fn as_borrowed(&'a self) -> Fragment<'a> {
		match self {
			Fragment::Owned(owned) => match owned {
				OwnedFragment::None => Fragment::None,
				OwnedFragment::Statement {
					text,
					line,
					column,
				} => Fragment::Borrowed(
					BorrowedFragment::Statement {
						text: text.as_str(),
						line: *line,
						column: *column,
					},
				),
				OwnedFragment::Internal {
					text,
				} => Fragment::Borrowed(
					BorrowedFragment::Internal {
						text: text.as_str(),
					},
				),
			},
			Fragment::Borrowed(b) => Fragment::Borrowed(*b), /* Copy since BorrowedFragment is Copy */
			Fragment::None => Fragment::None,
		}
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
pub trait IntoFragment<'a> {
	fn into_fragment(self) -> Fragment<'a>;
}

// Additional IntoFragment implementations for closure returning OwnedFragment
impl<F> IntoFragment<'static> for F
where
	F: Fn() -> OwnedFragment,
{
	fn into_fragment(self) -> Fragment<'static> {
		Fragment::Owned(self())
	}
}

// Implementation for &OwnedFragment
impl IntoFragment<'static> for &OwnedFragment {
	fn into_fragment(self) -> Fragment<'static> {
		Fragment::Owned(self.clone())
	}
}

/// Trait for lazy fragment generation that returns Fragment instead of
/// OwnedFragment
pub trait LazyFragment<'a>: Copy {
	fn fragment(&self) -> Fragment<'a>;
}

/// Wrapper to allow LazyFragment to be used as IntoFragment
pub struct LazyFragmentWrapper<T>(pub T);

impl<'a, T> IntoFragment<'a> for LazyFragmentWrapper<T>
where
	T: LazyFragment<'a>,
{
	fn into_fragment(self) -> Fragment<'a> {
		self.0.fragment()
	}
}

// Implementation for closures that return Fragment
impl<'a, F> LazyFragment<'a> for F
where
	F: Fn() -> Fragment<'a> + Copy,
{
	fn fragment(&self) -> Fragment<'a> {
		self()
	}
}

// Implementation for Fragment itself - using clone since we can't borrow with
// proper lifetime
impl<'a> LazyFragment<'a> for Fragment<'a>
where
	Fragment<'a>: Copy,
{
	fn fragment(&self) -> Fragment<'a> {
		self.clone()
	}
}

// Implementation for &Fragment
impl<'a, 'b> LazyFragment<'a> for &'b Fragment<'a>
where
	'a: 'b,
{
	fn fragment(&self) -> Fragment<'a> {
		(*self).clone()
	}
}

// Implementation for Fragment enum
impl<'a> IntoFragment<'a> for Fragment<'a> {
	fn into_fragment(self) -> Fragment<'a> {
		self
	}
}

// Implementation for &Fragment - creates a borrowed view
impl<'a> IntoFragment<'a> for &'a Fragment<'a> {
	fn into_fragment(self) -> Fragment<'a> {
		self.as_borrowed()
	}
}

// Implementation for OwnedFragment
impl IntoFragment<'_> for OwnedFragment {
	fn into_fragment(self) -> Fragment<'static> {
		Fragment::Owned(self)
	}
}

// Implementation for BorrowedFragment - converts to owned
impl<'a> IntoFragment<'a> for BorrowedFragment<'a> {
	fn into_fragment(self) -> Fragment<'a> {
		Fragment::Owned(self.into_owned())
	}
}

// Implementation for Option<OwnedFragment>
impl IntoFragment<'_> for Option<OwnedFragment> {
	fn into_fragment(self) -> Fragment<'static> {
		match self {
			Some(fragment) => Fragment::Owned(fragment),
			None => Fragment::None,
		}
	}
}

// Also provide From implementations for convenience
impl From<Option<OwnedFragment>> for OwnedFragment {
	fn from(fragment_opt: Option<OwnedFragment>) -> Self {
		match fragment_opt {
			Some(fragment) => fragment,
			None => OwnedFragment::None,
		}
	}
}

// String reference implementations - return borrowed fragments to avoid
// allocation
impl<'a> IntoFragment<'a> for &'a str {
	fn into_fragment(self) -> Fragment<'a> {
		Fragment::Borrowed(BorrowedFragment::Internal {
			text: self,
		})
	}
}

impl<'a> IntoFragment<'a> for &'a String {
	fn into_fragment(self) -> Fragment<'a> {
		Fragment::Borrowed(BorrowedFragment::Internal {
			text: self.as_str(),
		})
	}
}

impl IntoFragment<'_> for String {
	fn into_fragment(self) -> Fragment<'static> {
		Fragment::Owned(OwnedFragment::Internal {
			text: self,
		})
	}
}

// Serialize Fragment<'a> by converting to OwnedFragment
impl<'a> serde::Serialize for Fragment<'a> {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		self.clone().into_owned().serialize(serializer)
	}
}

// Deserialize always creates Fragment::Owned with OwnedFragment
impl<'de, 'a> serde::Deserialize<'de> for Fragment<'a> {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let owned = OwnedFragment::deserialize(deserializer)?;
		Ok(Fragment::Owned(owned))
	}
}

// PartialEq implementation for Fragment<'a>
impl<'a> PartialEq for Fragment<'a> {
	fn eq(&self, other: &Self) -> bool {
		self.value() == other.value()
			&& self.line() == other.line()
			&& self.column() == other.column()
	}
}

impl<'a> Eq for Fragment<'a> {}
