// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB







use crate::fragment::Fragment;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Declared<T> {
	pub value: T,
	pub fragment: Fragment,
}

impl<T> Declared<T> {
	pub fn new(value: T, fragment: Fragment) -> Self {
		Self {
			value,
			fragment,
		}
	}

	pub fn as_ref(&self) -> Declared<&T> {
		Declared {
			value: &self.value,
			fragment: self.fragment.clone(),
		}
	}

	pub fn map<U>(self, map: impl FnOnce(T) -> U) -> Declared<U> {
		Declared {
			value: map(self.value),
			fragment: self.fragment,
		}
	}

	pub fn into_value(self) -> T {
		self.value
	}
}

pub trait DeclaredOption<T> {
	fn declared_value(&self) -> Option<T>
	where
		T: Copy;

	fn declared_fragment(&self) -> Option<&Fragment>;
}

impl<T> DeclaredOption<T> for Option<Declared<T>> {
	fn declared_value(&self) -> Option<T>
	where
		T: Copy,
	{
		self.as_ref().map(|declared| declared.value)
	}

	fn declared_fragment(&self) -> Option<&Fragment> {
		self.as_ref().map(|declared| &declared.fragment)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn mapping_the_value_keeps_the_span() {
		// A map that rebuilt the value but dropped the fragment would turn a locatable
		// declaration into an unlocatable one, which is what this type exists to prevent.
		let declared = Declared::new(30u64, Fragment::statement("30s", 4, 9));

		let mapped = declared.map(|seconds| seconds * 1_000);

		assert_eq!(mapped.value, 30_000);
		assert_eq!(mapped.fragment.text(), "30s");
		assert_eq!(mapped.fragment.line().0, 4);
		assert_eq!(mapped.fragment.column().0, 9);
	}

	#[test]
	fn an_absent_declaration_has_neither_value_nor_span() {
		// No fragment for an absent declaration, so a diagnostic cannot be pointed at a key
		// the author never wrote.
		let absent: Option<Declared<u64>> = None;

		assert_eq!(absent.declared_value(), None);
		assert!(absent.declared_fragment().is_none());
	}

	#[test]
	fn a_present_declaration_exposes_value_and_span_separately() {
		// The two halves come back independently so a caller wanting only the value does not
		// have to unwrap past the fragment and drop it by habit.
		let present = Some(Declared::new(5u64, Fragment::internal("5")));

		assert_eq!(present.declared_value(), Some(5));
		assert_eq!(present.declared_fragment().map(Fragment::text), Some("5"));
	}
}
