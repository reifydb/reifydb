// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

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
	l > r
}

#[inline]
pub fn is_greater_than_equal(l: bool, r: bool) -> bool {
	l >= r
}

#[inline]
pub fn is_less_than(l: bool, r: bool) -> bool {
	l < r
}

#[inline]
pub fn is_less_than_equal(l: bool, r: bool) -> bool {
	l <= r
}

#[inline]
pub fn logical_and(l: bool, r: bool) -> bool {
	l && r
}

#[inline]
pub fn logical_or(l: bool, r: bool) -> bool {
	l || r
}

#[inline]
pub fn logical_not(b: bool) -> bool {
	!b
}

#[inline]
pub fn logical_xor(l: bool, r: bool) -> bool {
	l ^ r
}

#[cfg(test)]
mod tests {
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

	#[test]
	fn test_logical_and() {
		assert!(logical_and(true, true));
		assert!(!logical_and(true, false));
		assert!(!logical_and(false, true));
		assert!(!logical_and(false, false));
	}

	#[test]
	fn test_logical_or() {
		assert!(logical_or(true, true));
		assert!(logical_or(true, false));
		assert!(logical_or(false, true));
		assert!(!logical_or(false, false));
	}

	#[test]
	fn test_logical_not() {
		assert!(!logical_not(true));
		assert!(logical_not(false));
	}

	#[test]
	fn test_logical_xor() {
		assert!(!logical_xor(true, true));
		assert!(logical_xor(true, false));
		assert!(logical_xor(false, true));
		assert!(!logical_xor(false, false));
	}
}
