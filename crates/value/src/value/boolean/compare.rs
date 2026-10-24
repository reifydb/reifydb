// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[inline]
pub fn is_equal(l: bool, r: bool) -> bool {
	l == r
}

#[inline]
pub fn is_not_equal(l: bool, r: bool) -> bool {
	l != r
}

#[inline]
pub fn is_greater_than(l: bool, r: bool) -> bool {
	l & !r
}

#[inline]
pub fn is_greater_than_equal(l: bool, r: bool) -> bool {
	l >= r
}

#[inline]
pub fn is_less_than(l: bool, r: bool) -> bool {
	!l & r
}

#[inline]
pub fn is_less_than_equal(l: bool, r: bool) -> bool {
	l <= r
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_is_equal() {
		assert!(is_equal(true, true));
		assert!(is_equal(false, false));
		assert!(!is_equal(true, false));
		assert!(!is_equal(false, true));
	}

	#[test]
	fn test_is_not_equal() {
		assert!(!is_not_equal(true, true));
		assert!(!is_not_equal(false, false));
		assert!(is_not_equal(true, false));
		assert!(is_not_equal(false, true));
	}

	#[test]
	fn test_is_greater_than() {
		assert!(is_greater_than(true, false));
		assert!(!is_greater_than(false, true));
		assert!(!is_greater_than(true, true));
		assert!(!is_greater_than(false, false));
	}

	#[test]
	fn test_is_greater_than_equal() {
		assert!(is_greater_than_equal(true, false));
		assert!(!is_greater_than_equal(false, true));
		assert!(is_greater_than_equal(true, true));
		assert!(is_greater_than_equal(false, false));
	}

	#[test]
	fn test_is_less_than() {
		assert!(!is_less_than(true, false));
		assert!(is_less_than(false, true));
		assert!(!is_less_than(true, true));
		assert!(!is_less_than(false, false));
	}

	#[test]
	fn test_is_less_than_equal() {
		assert!(!is_less_than_equal(true, false));
		assert!(is_less_than_equal(false, true));
		assert!(is_less_than_equal(true, true));
		assert!(is_less_than_equal(false, false));
	}
}
