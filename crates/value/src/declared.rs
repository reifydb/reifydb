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
	// Intent: the fragment must survive a transformation of the value. A map that rebuilt the
	// value but dropped the span would silently turn a locatable declaration into an unlocatable
	// one, which is the exact regression this type exists to prevent.
	fn mapping_the_value_keeps_the_span() {
		let declared = Declared::new(30u64, Fragment::statement("30s", 4, 9));

		let mapped = declared.map(|seconds| seconds * 1_000);

		assert_eq!(mapped.value, 30_000);
		assert_eq!(mapped.fragment.text(), "30s");
		assert_eq!(mapped.fragment.line().0, 4);
		assert_eq!(mapped.fragment.column().0, 9);
	}

	#[test]
	// Intent: an absent declaration yields no value AND no span, so a caller cannot accidentally
	// point a diagnostic at a key the author never wrote.
	fn an_absent_declaration_has_neither_value_nor_span() {
		let absent: Option<Declared<u64>> = None;

		assert_eq!(absent.declared_value(), None);
		assert!(absent.declared_fragment().is_none());
	}

	#[test]
	// Intent: a present declaration hands back both halves independently, so a caller that only
	// needs the value does not have to reach past the span and lose it by habit.
	fn a_present_declaration_exposes_value_and_span_separately() {
		let present = Some(Declared::new(5u64, Fragment::internal("5")));

		assert_eq!(present.declared_value(), Some(5));
		assert_eq!(present.declared_fragment().map(Fragment::text), Some("5"));
	}
}
