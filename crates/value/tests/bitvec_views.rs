// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	borrow::Cow,
	panic::{AssertUnwindSafe, catch_unwind},
};

use postcard::{from_bytes, to_allocvec};
use reifydb_value::util::bitvec::BitVec;

const OFFSETS: [usize; 8] = [0, 1, 7, 8, 9, 63, 64, 65];
const LENGTHS: [usize; 8] = [0, 1, 7, 8, 9, 64, 65, 200];
const PARENT_LEN: usize = 300;

fn pattern(len: usize, seed: u64) -> Vec<bool> {
	let mut state = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
	(0..len).map(|_| {
		state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
		(state >> 33) & 1 == 1
	})
	.collect()
}

fn render(bits: &[bool]) -> String {
	bits.iter()
		.map(|&b| {
			if b {
				'1'
			} else {
				'0'
			}
		})
		.collect()
}

fn views() -> impl Iterator<Item = (usize, usize)> {
	OFFSETS.iter().flat_map(|&offset| LENGTHS.iter().map(move |&len| (offset, len)))
}

#[track_caller]
fn assert_reads_match(view: &BitVec, model: &[bool], ctx: &str) {
	let ones = model.iter().filter(|&&b| b).count();
	assert_eq!(view.len(), model.len(), "{ctx}: len");
	assert_eq!(view.is_empty(), model.is_empty(), "{ctx}: is_empty");
	for (i, &expected) in model.iter().enumerate() {
		assert_eq!(view.get(i), expected, "{ctx}: get({i})");
	}
	let iter = view.iter();
	assert_eq!(iter.len(), model.len(), "{ctx}: iter exact size");
	assert_eq!(iter.collect::<Vec<_>>(), model, "{ctx}: iter");
	assert_eq!(view.to_vec(), model, "{ctx}: to_vec");
	assert_eq!(view.count_ones(), ones, "{ctx}: count_ones");
	assert_eq!(view.count_zeros(), model.len() - ones, "{ctx}: count_zeros");
	assert_eq!(view.any(), ones > 0, "{ctx}: any");
	assert_eq!(view.none(), ones == 0, "{ctx}: none");
	assert_eq!(view.all_ones(), ones == model.len(), "{ctx}: all_ones");
	assert_eq!(format!("{}", view), render(model), "{ctx}: Display");
	assert_eq!(*view, BitVec::from_slice(model), "{ctx}: PartialEq with a fresh BitVec");
	let negated: Vec<bool> = model.iter().map(|b| !b).collect();
	let not = view.not();
	assert_eq!(not.to_vec(), negated, "{ctx}: not");
	assert_eq!(not.count_ones(), model.len() - ones, "{ctx}: not must never count bits past the view");
	assert_eq!(not.to_packed_bytes(), BitVec::from_slice(&negated).to_packed_bytes(), "{ctx}: not trailing bits");
}

#[test]
fn every_read_on_a_view_matches_the_model() {
	// Reads on a view must honour the bit offset, otherwise every non byte aligned batch reads the wrong rows.
	for seed in 0..3 {
		let bits = pattern(PARENT_LEN, seed);
		let parent = BitVec::from_slice(&bits);
		for (offset, len) in views() {
			let view = parent.slice(offset, offset + len);
			assert_reads_match(
				&view,
				&bits[offset..offset + len],
				&format!("seed {seed} view {offset}+{len}"),
			);
		}
	}
}

#[test]
fn reads_on_a_view_never_see_bits_outside_it() {
	// Bits just outside the view must never leak into counts or predicates, otherwise masks select phantom rows.
	for (offset, len) in views() {
		let outside_ones: Vec<bool> = (0..PARENT_LEN).map(|i| i < offset || i >= offset + len).collect();
		let view = BitVec::from_slice(&outside_ones).slice(offset, offset + len);
		assert_eq!(view.count_ones(), 0, "view {offset}+{len}");
		assert!(view.none(), "view {offset}+{len}");
		assert!(!view.any(), "view {offset}+{len}");
		assert_eq!(view.all_ones(), len == 0, "view {offset}+{len}");
		assert_eq!(view.not().count_ones(), len, "view {offset}+{len}");

		let inside_ones: Vec<bool> = outside_ones.iter().map(|b| !b).collect();
		let view = BitVec::from_slice(&inside_ones).slice(offset, offset + len);
		assert_eq!(view.count_ones(), len, "view {offset}+{len}");
		assert!(view.all_ones(), "view {offset}+{len}");
		assert_eq!(view.any(), len > 0, "view {offset}+{len}");
		assert_eq!(view.not().count_ones(), 0, "view {offset}+{len}");
		assert!(view.not().none(), "view {offset}+{len}");
	}
}

#[test]
fn nested_views_offset_from_the_outer_view() {
	// A view of a view must add both offsets exactly, never restart from the parent's bit zero.
	let bits = pattern(PARENT_LEN, 11);
	let parent = BitVec::from_slice(&bits);
	for &outer in &OFFSETS {
		let outer_view = parent.slice(outer, outer + 220);
		for &inner in &[0usize, 1, 3, 8, 13, 64] {
			let inner_view = outer_view.slice(inner, inner + 70);
			let start = outer + inner;
			assert_reads_match(&inner_view, &bits[start..start + 70], &format!("nested {outer}+{inner}"));
		}
	}
}

#[test]
fn take_is_a_prefix_view() {
	// take must return exactly the first n bits and clamp past the end.
	let bits = pattern(PARENT_LEN, 5);
	let parent = BitVec::from_slice(&bits);
	for &n in &LENGTHS {
		assert_reads_match(&parent.take(n), &bits[..n], &format!("take {n}"));
	}
	assert_eq!(parent.take(PARENT_LEN + 50).len(), PARENT_LEN);
	let view = parent.slice(9, 100);
	assert_reads_match(&view.take(20), &bits[9..29], "take of a view");
	assert_eq!(view.take(500).len(), 91, "take of a view must clamp to the view length");
}

#[test]
fn slice_clamps_out_of_range_bounds() {
	// Out of range bounds must clamp to the view, never reach into bits beyond it.
	let bits = pattern(PARENT_LEN, 2);
	let parent = BitVec::from_slice(&bits);
	assert_reads_match(&parent.slice(290, 1000), &bits[290..], "tail clamp");
	assert!(parent.slice(400, 500).is_empty());
	assert!(parent.slice(50, 10).is_empty());
	let view = parent.slice(10, 20);
	assert_reads_match(&view.slice(5, 100), &bits[15..20], "nested clamp");
	assert!(view.slice(10, 12).is_empty());
}

#[test]
fn slices_and_takes_share_the_parent_allocation() {
	// A view must share the parent's bytes, otherwise slicing a validity bitmap copies it per batch.
	let mut parent = BitVec::from_slice(&pattern(PARENT_LEN, 3));
	let base = parent.to_packed_bytes().as_ptr();
	parent.set(0, !parent.get(0));
	assert_eq!(parent.to_packed_bytes().as_ptr(), base, "a fresh bitvec must be unique and write in place");
	let view = parent.slice(9, 100);
	assert_eq!(view.capacity(), parent.capacity(), "an offset view must read the parent allocation, never a copy");
	drop(view);
	parent.set(0, !parent.get(0));
	assert_eq!(parent.to_packed_bytes().as_ptr(), base, "dropping the only view must make the parent unique again");

	let head = parent.take(65);
	let parent_bytes = parent.to_packed_bytes();
	let head_bytes = head.to_packed_bytes();
	assert!(matches!(parent_bytes, Cow::Borrowed(_)));
	assert!(matches!(head_bytes, Cow::Borrowed(_)), "a view at bit zero must borrow its bytes");
	assert_eq!(head_bytes.as_ptr(), parent_bytes.as_ptr());
	assert_eq!(head_bytes.len(), 9);
}

#[test]
fn packed_bytes_borrow_at_bit_zero_and_copy_otherwise() {
	// A view off bit zero must repack into fresh bytes whose trailing bits are exactly zero.
	let bits = pattern(PARENT_LEN, 7);
	let parent = BitVec::from_slice(&bits);
	for (offset, len) in views() {
		let view = parent.slice(offset, offset + len);
		let fresh = BitVec::from_slice(&bits[offset..offset + len]);
		let bytes = view.to_packed_bytes();
		assert_eq!(bytes.len(), len.div_ceil(8), "view {offset}+{len}: byte length");
		if offset == 0 {
			assert!(matches!(bytes, Cow::Borrowed(_)), "view {offset}+{len} must borrow");
			for i in 0..len {
				assert_eq!((bytes[i / 8] >> (i % 8)) & 1 == 1, bits[i], "view {offset}+{len}: bit {i}");
			}
		} else {
			assert!(matches!(bytes, Cow::Owned(_)), "view {offset}+{len} must repack");
			assert_eq!(&bytes[..], &fresh.to_packed_bytes()[..], "view {offset}+{len}: repacked bytes");
			if len % 8 != 0 {
				let last = bytes[len / 8];
				assert_eq!(last >> (len % 8), 0, "view {offset}+{len}: trailing bits must be zero");
			}
		}
	}
}

#[test]
fn packed_bytes_of_a_fresh_bitvec_have_zero_trailing_bits() {
	// A whole BitVec must pack with zero trailing bits, otherwise Arrow consumers that popcount bytes miscount.
	for &len in &LENGTHS {
		let bv = BitVec::repeat(len, true);
		let bytes = bv.to_packed_bytes();
		assert_eq!(bytes.len(), len.div_ceil(8));
		if len % 8 != 0 {
			assert_eq!(bytes[len / 8] >> (len % 8), 0, "len {len}");
		}
		let ones: u32 = bytes.iter().map(|b| b.count_ones()).sum();
		assert_eq!(ones as usize, len, "len {len}");
	}
}

#[test]
fn and_or_between_views_with_different_offsets() {
	// Combining views at different bit offsets must align them bit by bit, never byte by byte.
	let left_bits = pattern(PARENT_LEN, 21);
	let right_bits = pattern(PARENT_LEN, 22);
	let left_parent = BitVec::from_slice(&left_bits);
	let right_parent = BitVec::from_slice(&right_bits);
	for &lo in &OFFSETS {
		for &ro in &OFFSETS {
			for &len in &LENGTHS {
				let left = left_parent.slice(lo, lo + len);
				let right = right_parent.slice(ro, ro + len);
				let l = &left_bits[lo..lo + len];
				let r = &right_bits[ro..ro + len];
				let and: Vec<bool> = l.iter().zip(r).map(|(a, b)| *a && *b).collect();
				let or: Vec<bool> = l.iter().zip(r).map(|(a, b)| *a || *b).collect();
				let ctx = format!("left {lo} right {ro} len {len}");
				assert_eq!(left.and(&right).to_vec(), and, "{ctx}: and");
				assert_eq!(left.or(&right).to_vec(), or, "{ctx}: or");
				assert_eq!(left.and(&right).count_ones(), and.iter().filter(|&&b| b).count(), "{ctx}");
				assert_eq!(left.or(&right).count_ones(), or.iter().filter(|&&b| b).count(), "{ctx}");
				assert_eq!(
					left.or(&right).to_packed_bytes(),
					BitVec::from_slice(&or).to_packed_bytes(),
					"{ctx}: or trailing bits"
				);
				let fresh_right = BitVec::from_slice(r);
				assert_eq!(left.and(&fresh_right).to_vec(), and, "{ctx}: view and fresh");
			}
		}
	}
}

#[test]
#[should_panic(expected = "assertion `left == right` failed")]
fn and_between_views_of_different_lengths_panics() {
	// Views of different lengths must be rejected, never silently truncated.
	let parent = BitVec::from_slice(&pattern(PARENT_LEN, 1));
	let _ = parent.slice(1, 10).and(&parent.slice(1, 11));
}

#[test]
fn equality_ignores_the_offset() {
	// Two views with the same bits must compare equal whatever their offsets, otherwise sliced columns look
	// different.
	let bits: Vec<bool> = (0..PARENT_LEN).map(|i| i % 3 == 0).collect();
	let parent = BitVec::from_slice(&bits);
	assert_eq!(parent.slice(0, 90), parent.slice(3, 93));
	assert_eq!(parent.slice(1, 70), parent.slice(64, 133));
	assert_ne!(parent.slice(0, 90), parent.slice(1, 91));
	assert_ne!(parent.slice(0, 90), parent.slice(0, 89));
}

#[test]
fn writes_on_a_view_never_leak_into_the_parent() {
	// Every write on a view must copy first, otherwise a batch write corrupts the shared block bitmap.
	let bits = pattern(PARENT_LEN, 9);
	let parent = BitVec::from_slice(&bits);
	for (offset, len) in views() {
		let model = &bits[offset..offset + len];
		let ctx = format!("view {offset}+{len}");

		let mut pushed = parent.slice(offset, offset + len);
		let next = !bits[offset + len];
		pushed.push(next);
		let mut expected = model.to_vec();
		expected.push(next);
		assert_eq!(pushed.to_vec(), expected, "{ctx}: push");
		assert_eq!(parent.to_vec(), bits, "{ctx}: push must never write the parent bit after the view");

		let other = BitVec::from_slice(&pattern(13, 4));
		let mut extended = parent.slice(offset, offset + len);
		extended.extend(&other);
		let mut expected = model.to_vec();
		expected.extend(other.to_vec());
		assert_eq!(extended.to_vec(), expected, "{ctx}: extend");
		assert_eq!(parent.to_vec(), bits, "{ctx}: extend must leave the parent intact");

		let mut cleared = parent.slice(offset, offset + len);
		cleared.clear();
		assert!(cleared.is_empty(), "{ctx}: clear");
		assert_eq!(parent.to_vec(), bits, "{ctx}: clear must leave the parent intact");
		cleared.push(true);
		assert_eq!(cleared.to_vec(), vec![true], "{ctx}: push after clear");

		if len == 0 {
			continue;
		}

		let mut set = parent.slice(offset, offset + len);
		set.set(0, !model[0]);
		set.set(len - 1, !model[len - 1]);
		let mut expected = model.to_vec();
		expected[0] = !model[0];
		expected[len - 1] = !model[len - 1];
		assert_eq!(set.to_vec(), expected, "{ctx}: set");
		assert_eq!(parent.to_vec(), bits, "{ctx}: set must never write through to the parent");

		let mut reordered = parent.slice(offset, offset + len);
		let indices: Vec<usize> = (0..len).rev().collect();
		reordered.reorder(&indices);
		let expected: Vec<bool> = model.iter().rev().copied().collect();
		assert_eq!(reordered.to_vec(), expected, "{ctx}: reorder");
		assert_eq!(parent.to_vec(), bits, "{ctx}: reorder must leave the parent intact");
	}
}

#[test]
fn writes_on_the_parent_never_leak_into_a_view() {
	// A view taken before a parent write must keep the bits it saw, otherwise earlier batches change under readers.
	let bits = pattern(PARENT_LEN, 10);
	for (offset, len) in views() {
		let mut parent = BitVec::from_slice(&bits);
		let view = parent.slice(offset, offset + len);
		if len > 0 {
			parent.set(offset, !bits[offset]);
		}
		parent.push(true);
		let other = BitVec::repeat(10, true);
		parent.extend(&other);
		assert_eq!(view.to_vec(), &bits[offset..offset + len], "view {offset}+{len}");
		parent.clear();
		assert_eq!(view.to_vec(), &bits[offset..offset + len], "view {offset}+{len} after parent clear");
	}
}

#[test]
fn writes_on_a_unique_view_are_correct_after_the_parent_is_dropped() {
	// A unique view must normalize before writing, otherwise it writes at the parent's bit positions.
	let bits = pattern(PARENT_LEN, 12);
	for (offset, len) in views() {
		let ctx = format!("view {offset}+{len}");
		let parent = BitVec::from_slice(&bits);
		let mut view = parent.slice(offset, offset + len);
		drop(parent);
		let bit_zero = (offset == 0).then(|| view.to_packed_bytes().as_ptr());
		let mut expected = bits[offset..offset + len].to_vec();
		view.push(true);
		expected.push(true);
		view.push(false);
		expected.push(false);
		view.set(0, !expected[0]);
		expected[0] = !expected[0];
		assert_eq!(view.to_vec(), expected, "{ctx}");
		assert_eq!(view.count_ones(), expected.iter().filter(|&&b| b).count(), "{ctx}");
		assert_eq!(view.to_packed_bytes(), BitVec::from_slice(&expected).to_packed_bytes(), "{ctx}: bytes");
		if let Some(before) = bit_zero {
			assert_eq!(
				view.to_packed_bytes().as_ptr(),
				before,
				"{ctx}: a unique view at bit zero must write in place"
			);
		}
	}
}

#[test]
fn extend_from_a_view_reads_at_its_offset() {
	// Extending from a view must copy the view's bits, never the bits at the start of its parent.
	let bits = pattern(PARENT_LEN, 14);
	let parent = BitVec::from_slice(&bits);
	for (offset, len) in views() {
		let prefix = pattern(11, 15);
		let mut target = BitVec::from_slice(&prefix);
		target.extend(&parent.slice(offset, offset + len));
		let mut expected = prefix.clone();
		expected.extend_from_slice(&bits[offset..offset + len]);
		assert_eq!(target.to_vec(), expected, "view {offset}+{len}");
		assert_eq!(target.count_ones(), expected.iter().filter(|&&b| b).count(), "view {offset}+{len}");
	}
}

#[test]
fn get_past_the_view_end_panics_even_when_the_parent_has_the_bit() {
	// Indexing must be bounded by the view, never by the parent allocation.
	let parent = BitVec::repeat(PARENT_LEN, true);
	let view = parent.slice(9, 14);
	assert!(catch_unwind(AssertUnwindSafe(|| view.get(5))).is_err(), "get past the view must panic");
	let mut writable = parent.slice(9, 14);
	assert!(catch_unwind(AssertUnwindSafe(|| writable.set(5, false))).is_err(), "set past the view must panic");
	assert!(parent.all_ones(), "a rejected set must never touch the parent");
}

#[test]
fn serialize_of_a_view_equals_a_fresh_bitvec() {
	// A view must serialize exactly like a fresh BitVec, otherwise sliced columns change the wire format.
	let bits = pattern(PARENT_LEN, 16);
	let parent = BitVec::from_slice(&bits);
	for (offset, len) in views() {
		let view = parent.slice(offset, offset + len);
		let fresh = BitVec::from_slice(&bits[offset..offset + len]);
		let encoded = to_allocvec(&view).unwrap();
		assert_eq!(encoded, to_allocvec(&fresh).unwrap(), "view {offset}+{len}");
		let decoded: BitVec = from_bytes(&encoded).unwrap();
		assert_eq!(decoded.to_vec(), &bits[offset..offset + len], "view {offset}+{len}: round trip");
	}
	let whole = parent.slice(0, PARENT_LEN);
	assert_eq!(to_allocvec(&whole).unwrap(), to_allocvec(&BitVec::from_slice(&bits)).unwrap());
}

#[test]
fn debug_of_a_view_equals_a_fresh_bitvec() {
	// Debug must render only the view's bits, never the parent's bytes or length.
	let bits = pattern(PARENT_LEN, 17);
	let parent = BitVec::from_slice(&bits);
	for (offset, len) in views() {
		let view = parent.slice(offset, offset + len);
		let fresh = BitVec::from_slice(&bits[offset..offset + len]);
		assert_eq!(format!("{:?}", view), format!("{:?}", fresh), "view {offset}+{len}");
	}
}
