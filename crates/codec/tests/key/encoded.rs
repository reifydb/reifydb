// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::Bound;

use reifydb_codec::key::encoded::EncodedKey;

macro_rules! as_key {
	($key:expr) => {{ EncodedKey::new(reifydb_codec::key::serialize(&$key)) }};
}

mod prefix {
	use std::ops::Bound;

	use reifydb_codec::key::encoded::EncodedKeyRange;

	use super::{excluded, included};

	#[test]
	fn test_simple() {
		let range = EncodedKeyRange::prefix(&[0x12, 0x34]);
		assert_eq!(range.start, included(&[0x12, 0x34]));
		assert_eq!(range.end, excluded(&[0x12, 0x35]));
	}

	#[test]
	fn test_with_trailing_ff() {
		let range = EncodedKeyRange::prefix(&[0x12, 0xff]);
		assert_eq!(range.start, included(&[0x12, 0xff]));
		assert_eq!(range.end, excluded(&[0x13]));
	}

	#[test]
	fn test_with_multiple_trailing_ff() {
		let range = EncodedKeyRange::prefix(&[0x12, 0xff, 0xff]);
		assert_eq!(range.start, included(&[0x12, 0xff, 0xff]));
		assert_eq!(range.end, excluded(&[0x13]));
	}

	#[test]
	fn test_all_ff() {
		let range = EncodedKeyRange::prefix(&[0xff, 0xff]);
		assert_eq!(range.start, included(&[0xff, 0xff]));
		assert_eq!(range.end, Bound::Unbounded);
	}

	#[test]
	fn test_empty() {
		let range = EncodedKeyRange::prefix(&[]);
		assert_eq!(range.start, included(&[]));
		assert_eq!(range.end, Bound::Unbounded);
	}

	#[test]
	fn test_mid_increment() {
		let range = EncodedKeyRange::prefix(&[0x12, 0x00, 0xff]);
		assert_eq!(range.start, included(&[0x12, 0x00, 0xff]));
		assert_eq!(range.end, excluded(&[0x12, 0x01]));
	}
}

mod start_end {
	use std::ops::Bound;

	use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};

	use super::included;

	#[test]
	fn test_start_and_end() {
		let range = EncodedKeyRange::start_end(Some(as_key!(1)), Some(as_key!(2)));
		assert_eq!(range.start, included(&as_key!(1)));
		assert_eq!(range.end, included(&as_key!(2)));
	}

	#[test]
	fn test_start_only() {
		let range = EncodedKeyRange::start_end(Some(as_key!(1)), None);
		assert_eq!(range.start, included(&as_key!(1)));
		assert_eq!(range.end, Bound::Unbounded);
	}

	#[test]
	fn test_end_only() {
		let range = EncodedKeyRange::start_end(None, Some(as_key!(2)));
		assert_eq!(range.start, Bound::Unbounded);
		assert_eq!(range.end, included(&as_key!(2)));
	}

	#[test]
	fn test_unbounded_range() {
		let range = EncodedKeyRange::start_end(None, None);
		assert_eq!(range.start, Bound::Unbounded);
		assert_eq!(range.end, Bound::Unbounded);
	}

	#[test]
	fn test_full_byte_range() {
		let range = EncodedKeyRange::start_end(Some(as_key!(0x00)), Some(as_key!(0xff)));
		assert_eq!(range.start, included(&as_key!(0x00)));
		assert_eq!(range.end, included(&as_key!(0xff)));
	}

	#[test]
	fn test_identical_bounds() {
		let range = EncodedKeyRange::start_end(Some(as_key!(0x42)), Some(as_key!(0x42)));
		assert_eq!(range.start, included(&as_key!(0x42)));
		assert_eq!(range.end, included(&as_key!(0x42)));
	}
}

mod all {
	use std::ops::Bound;

	use reifydb_codec::key::encoded::EncodedKeyRange;

	#[test]
	fn test_is_unbounded() {
		let range = EncodedKeyRange::all();
		assert_eq!(range.start, Bound::Unbounded);
		assert_eq!(range.end, Bound::Unbounded);
	}
}

mod parse {
	use std::ops::Bound;

	use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};

	use super::{excluded, included};

	#[test]
	fn test_full_range() {
		let r = EncodedKeyRange::parse("a..z");
		assert_eq!(r.start, included(b"a"));
		assert_eq!(r.end, excluded(b"z"));
	}

	#[test]
	fn test_inclusive_end() {
		let r = EncodedKeyRange::parse("a..=z");
		assert_eq!(r.start, included(b"a"));
		assert_eq!(r.end, included(b"z"));
	}

	#[test]
	fn test_unbounded_start() {
		let r = EncodedKeyRange::parse("..z");
		assert_eq!(r.start, Bound::Unbounded);
		assert_eq!(r.end, excluded(b"z"));
	}

	#[test]
	fn test_unbounded_end() {
		let r = EncodedKeyRange::parse("a..");
		assert_eq!(r.start, included(b"a"));
		assert_eq!(r.end, Bound::Unbounded);
	}

	#[test]
	fn test_inclusive_only() {
		let r = EncodedKeyRange::parse("..=z");
		assert_eq!(r.start, Bound::Unbounded);
		assert_eq!(r.end, included(b"z"));
	}

	#[test]
	fn test_invalid_string_returns_degenerate_range() {
		let r = EncodedKeyRange::parse("not a range");
		let expected = EncodedKey::new([0xff]);
		assert_eq!(r.start, Bound::Included(expected.clone()));
		assert_eq!(r.end, Bound::Excluded(expected));
	}

	#[test]
	fn test_empty_string_returns_degenerate_range() {
		let r = EncodedKeyRange::parse("");
		let expected = EncodedKey::new([0xff]);
		assert_eq!(r.start, Bound::Included(expected.clone()));
		assert_eq!(r.end, Bound::Excluded(expected));
	}

	#[test]
	fn test_binary_encoded_row() {
		// not a hex parse: only chars in 0x80..=0xff are taken as raw bytes, the rest pass through as UTF-8.
		let r = EncodedKeyRange::parse("0101..=0aff");
		assert_eq!(r.start, included(b"0101"));
		assert_eq!(r.end, included(b"0aff"));
	}
}

fn included(key: &[u8]) -> Bound<EncodedKey> {
	Bound::Included(EncodedKey::new(key))
}

fn excluded(key: &[u8]) -> Bound<EncodedKey> {
	Bound::Excluded(EncodedKey::new(key))
}
