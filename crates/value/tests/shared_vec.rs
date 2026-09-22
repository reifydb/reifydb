// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_value::util::shared_vec::SharedVec;

fn frozen_range(n: i64) -> SharedVec<i64> {
	let mut v = SharedVec::from_vec((0..n).collect());
	v.freeze();
	v
}

fn points_into<T>(parent: &[T], child: &[T]) -> bool {
	let start = parent.as_ptr() as usize;
	let end = start + parent.len() * size_of::<T>();
	let child_start = child.as_ptr() as usize;
	let child_end = child_start + child.len() * size_of::<T>();
	child_start >= start && child_end <= end
}

#[test]
fn freeze_is_zero_copy() {
	// Freezing must move the Vec into the Arc, never copy it, otherwise every Column construction pays a full copy.
	let mut v = SharedVec::from_vec((0..1000i64).collect());
	let before = v.as_ptr();
	v.freeze();
	assert_eq!(v.as_ptr(), before, "freeze must keep the original allocation");
	assert_eq!(v.len(), 1000);
	assert_eq!(&v[..], &(0..1000i64).collect::<Vec<_>>()[..]);
}

#[test]
fn freeze_is_idempotent() {
	// A second freeze must neither copy nor re-wrap the storage, otherwise it would break sharing with clones.
	let mut v = frozen_range(64);
	let before = v.as_ptr();
	let clone = v.clone();
	v.freeze();
	v.freeze();
	assert_eq!(v.as_ptr(), before);
	assert_eq!(clone.as_ptr(), before);
	assert_eq!(v, clone);
	v.make_mut()[0] = -1;
	assert_ne!(v.as_ptr(), before, "re-freezing must not detach the handle from its clone");
	assert_eq!(clone[0], 0);
}

#[test]
fn freeze_of_empty_is_valid() {
	// An empty frozen vector must behave as an empty slice and still accept writes.
	let mut v: SharedVec<i64> = SharedVec::from_vec(Vec::new());
	v.freeze();
	assert!(v.is_empty());
	let mut spare: SharedVec<i64> = SharedVec::with_capacity(8);
	spare.freeze();
	let spare_ptr = spare.as_ptr();
	assert_eq!(spare.make_mut().as_ptr(), spare_ptr, "a unique empty frozen handle must thaw in place, never copy");
	let c = v.clone();
	assert!(c.is_empty());
	v.push(7);
	assert_eq!(&v[..], &[7]);
	assert!(c.is_empty(), "a push through one handle must never show through an empty clone");
}

#[test]
fn clone_of_frozen_shares_the_same_pointer() {
	// A frozen clone must be an Arc bump that points at the exact same data.
	let mut v = frozen_range(10_000);
	let base = v.as_ptr();
	let c = v.clone();
	assert_eq!(c.as_ptr(), v.as_ptr());
	assert_eq!(c.len(), v.len());
	drop(c);
	assert_eq!(v.make_mut().as_ptr(), base, "dropping the only other handle must make the storage unique again");
}

#[test]
fn clone_of_owned_is_a_frozen_copy() {
	// Cloning an owned vector must copy exactly once and hand back a frozen value, otherwise a clone of the clone
	// copies again.
	let mut v: SharedVec<i64> = SharedVec::with_capacity(256);
	v.extend_from_slice(&(0..100).collect::<Vec<_>>());
	let mut c = v.clone();
	assert_ne!(c.as_ptr(), v.as_ptr(), "an owned clone must not alias the owned storage");
	assert_eq!(c, v);

	let cc = c.clone();
	assert_eq!(cc.as_ptr(), c.as_ptr(), "the clone of an owned vector must already be frozen");

	let owned_ptr = v.as_ptr();
	v.push(100);
	assert_eq!(v.as_ptr(), owned_ptr, "the source must stay owned and grow in place within its capacity");
	assert_eq!(c.len(), 100, "a push on the owned source must never show through its frozen clone");

	drop(cc);
	let frozen_ptr = c.as_ptr();
	assert_eq!(
		c.make_mut().as_ptr(),
		frozen_ptr,
		"the frozen clone must be the sole owner once its own clone is gone"
	);
}

#[test]
fn slice_of_frozen_points_into_the_parent() {
	// A slice of frozen storage must be O(1): same allocation, shifted by exactly the start offset.
	let v = frozen_range(100);
	let s = v.slice(10, 20);
	assert_eq!(s.as_ptr(), v.as_ptr().wrapping_add(10));
	assert_eq!(&s[..], &(10..20).collect::<Vec<_>>()[..]);
	assert!(points_into(&v, &s));
}

#[test]
fn slice_of_a_slice_offsets_from_the_view() {
	// A nested slice must offset from the view start, never from the allocation start.
	let v = frozen_range(100);
	let s = v.slice(10, 50);
	let ss = s.slice(5, 15);
	assert_eq!(ss.as_ptr(), v.as_ptr().wrapping_add(15));
	assert_eq!(&ss[..], &(15..25).collect::<Vec<_>>()[..]);
}

#[test]
fn slice_of_owned_copies_the_range_and_freezes_it() {
	// Slicing owned storage must copy only the range, and the copy must be frozen so it shares on clone.
	let v = SharedVec::from_vec((0..100i64).collect());
	let s = v.slice(10, 20);
	assert!(!points_into(&v, &s), "a slice of owned storage must not alias the owned buffer");
	assert_eq!(&s[..], &(10..20).collect::<Vec<_>>()[..]);
	let c = s.clone();
	assert_eq!(c.as_ptr(), s.as_ptr(), "the owned slice result must already be frozen");
}

#[test]
fn slice_clamps_every_out_of_range_request() {
	// Out of range bounds must clamp to the view length, never panic and never read past the view.
	let v = frozen_range(100);
	assert_eq!(&v.slice(90, 200)[..], &(90..100).collect::<Vec<_>>()[..]);
	assert!(v.slice(200, 300).is_empty());
	assert!(v.slice(50, 10).is_empty());
	assert_eq!(v.slice(0, usize::MAX).len(), 100);
	assert!(v.slice(100, 100).is_empty());

	let s = v.slice(10, 20);
	let clamped = s.slice(5, 1000);
	assert_eq!(
		&clamped[..],
		&(15..20).collect::<Vec<_>>()[..],
		"a nested slice must clamp to the view, not the parent"
	);
	assert!(s.slice(10, 11).is_empty());
	assert!(s.slice(11, 1000).is_empty());

	let owned = SharedVec::from_vec((0..10i64).collect());
	assert_eq!(&owned.slice(8, 50)[..], &[8, 9]);
	assert!(owned.slice(20, 30).is_empty());
}

#[test]
fn make_mut_on_unique_unsliced_storage_keeps_the_allocation() {
	// Thawing unique whole storage must hand back the original Vec with its capacity, never a copy.
	let mut vec = Vec::with_capacity(4096);
	vec.extend(0..100i64);
	let cap = vec.capacity();
	let mut v = SharedVec::from_vec(vec);
	let before = v.as_ptr();
	v.freeze();
	let inner = v.make_mut();
	assert_eq!(inner.as_ptr(), before);
	assert_eq!(inner.capacity(), cap, "thaw must keep the spare capacity of the original Vec");
	inner.push(100);
	assert_eq!(v.as_ptr(), before);
	assert_eq!(v.len(), 101);
}

#[test]
fn make_mut_on_unique_sliced_storage_drains_in_place() {
	// A unique slice must thaw by draining the original allocation, so its data starts at the allocation start.
	let parent = frozen_range(100);
	let base = parent.as_ptr();
	let mut s = parent.slice(10, 20);
	drop(parent);
	let inner = s.make_mut();
	assert_eq!(inner.as_ptr(), base, "the drained data must reuse the original allocation");
	assert_eq!(&inner[..], &(10..20).collect::<Vec<_>>()[..]);
	assert_eq!(s.len(), 10);

	let parent = frozen_range(100);
	let base = parent.as_ptr();
	let mut head = parent.slice(0, 7);
	drop(parent);
	assert_eq!(head.make_mut().as_ptr(), base);
	assert_eq!(&head[..], &(0..7).collect::<Vec<_>>()[..], "a head slice must truncate the tail exactly");
}

#[test]
fn make_mut_on_shared_storage_copies_and_leaves_the_other_handle_untouched() {
	// A write through one handle must never show through another handle on the same allocation.
	let mut v = frozen_range(100);
	let base = v.as_ptr();
	let mut c = v.clone();
	c.make_mut()[0] = 999;
	assert_ne!(c.as_ptr(), v.as_ptr(), "a shared thaw must copy");
	assert_eq!(v[0], 0);
	assert_eq!(&v[..], &(0..100).collect::<Vec<_>>()[..]);
	assert_eq!(c[0], 999);
	assert_eq!(&c[1..], &(1..100).collect::<Vec<_>>()[..]);
	assert_eq!(v.make_mut().as_ptr(), base, "the thawed handle must release its reference to the shared storage");
}

#[test]
fn make_mut_on_a_shared_slice_copies_only_the_view() {
	// A shared slice must thaw into a copy of exactly its view, never the whole parent.
	let parent = frozen_range(100);
	let mut s = parent.slice(40, 45);
	s.make_mut()[0] = -1;
	assert!(!points_into(&parent, &s));
	assert_eq!(&s[..], &[-1, 41, 42, 43, 44]);
	assert_eq!(parent[40], 40, "the parent must never see a write made through its slice");
	assert_eq!(parent.len(), 100);
}

#[test]
fn push_and_extend_after_clone_never_leak_into_the_other_handle() {
	// Growth through one handle must never change the length or content seen by another.
	let v = frozen_range(100);
	let mut a = v.clone();
	let mut b = v.clone();
	a.push(100);
	b.extend_from_slice(&[7, 8, 9]);
	assert_eq!(v.len(), 100);
	assert_eq!(&v[..], &(0..100).collect::<Vec<_>>()[..]);
	assert_eq!(a.len(), 101);
	assert_eq!(a[100], 100);
	assert_eq!(b.len(), 103);
	assert_eq!(&b[100..], &[7, 8, 9]);

	let mut s = v.slice(0, 3);
	s.push(-5);
	assert_eq!(&s[..], &[0, 1, 2, -5]);
	assert_eq!(v[3], 3, "a push on a head slice must never overwrite the parent row after the view");
}

#[test]
fn owned_capacity_is_the_vec_capacity() {
	// An owned vector must report the real Vec capacity so pooled buffers can be sized.
	let v: SharedVec<u64> = SharedVec::with_capacity(300);
	assert!(v.capacity() >= 300);
	assert!(v.is_empty());
}

#[test]
fn a_write_copies_exactly_while_another_clone_or_slice_is_alive() {
	// A write must thaw in place exactly when no clone or slice holds the allocation, otherwise it corrupts a live
	// reader or copies for nothing.
	let mut owned = SharedVec::from_vec(vec![1u32, 2, 3]);
	let base = owned.as_ptr();
	assert_eq!(owned.make_mut().as_ptr(), base, "owned storage is never shared");
	owned.freeze();
	assert_eq!(owned.make_mut().as_ptr(), base, "a unique frozen handle is not shared");

	owned.freeze();
	let slice = owned.slice(1, 2);
	owned.make_mut()[1] = 20;
	assert_ne!(owned.as_ptr(), base, "a live slice must keep the allocation shared");
	assert_eq!(&slice[..], &[2]);
	drop(slice);

	owned.freeze();
	let base = owned.as_ptr();
	let slice = owned.slice(1, 2);
	let clone = owned.clone();
	drop(slice);
	owned.make_mut()[1] = 30;
	assert_ne!(owned.as_ptr(), base, "a live clone must keep the allocation shared after the slice is gone");
	assert_eq!(&clone[..], &[1, 20, 3]);
	drop(clone);

	owned.freeze();
	let base = owned.as_ptr();
	let slice = owned.slice(1, 2);
	let clone = owned.clone();
	drop(slice);
	drop(clone);
	assert_eq!(
		owned.make_mut().as_ptr(),
		base,
		"dropping every clone and slice must make the storage unique again"
	);
}

#[test]
fn equality_compares_contents_not_representation() {
	// Owned, frozen and sliced values with the same elements must compare equal, otherwise freezing changes query
	// results.
	let owned = SharedVec::from_vec((10..20i64).collect());
	let frozen = frozen_range(30).slice(10, 20);
	let mut frozen_whole = SharedVec::from_vec((10..20i64).collect());
	frozen_whole.freeze();
	assert_eq!(owned, frozen);
	assert_eq!(frozen, frozen_whole);
	assert_ne!(owned, frozen_range(30).slice(11, 21));
	assert_ne!(owned, frozen_range(30).slice(10, 19));
}

#[test]
fn debug_equals_slice_debug_for_every_representation() {
	// Debug must render exactly like a Vec of the visible elements, never the hidden parent storage.
	let expected = format!("{:?}", (10..20i64).collect::<Vec<_>>());
	let owned = SharedVec::from_vec((10..20i64).collect());
	let mut frozen = SharedVec::from_vec((10..20i64).collect());
	frozen.freeze();
	let sliced = frozen_range(100).slice(10, 20);
	assert_eq!(format!("{:?}", owned), expected);
	assert_eq!(format!("{:?}", frozen), expected);
	assert_eq!(format!("{:?}", sliced), expected);
	assert_eq!(format!("{:?}", frozen_range(10).slice(5, 5)), "[]");
}

#[test]
fn thaw_drops_every_element_outside_the_view_exactly_once() {
	// Draining a unique slice must drop the elements outside the view exactly once, never leak or double drop them.
	let probe = Arc::new(0u8);
	let mut v = SharedVec::from_vec((0..10).map(|_| Arc::clone(&probe)).collect::<Vec<_>>());
	v.freeze();
	assert_eq!(Arc::strong_count(&probe), 11);
	let mut s = v.slice(2, 5);
	drop(v);
	assert_eq!(Arc::strong_count(&probe), 11, "the slice must keep the whole allocation alive until thaw");
	s.make_mut();
	assert_eq!(Arc::strong_count(&probe), 4);
	assert_eq!(s.len(), 3);
	drop(s);
	assert_eq!(Arc::strong_count(&probe), 1);
}

#[test]
fn shared_thaw_clones_only_the_view() {
	// A shared thaw must clone exactly the view elements and leave the parent's elements alive exactly once.
	let probe = Arc::new(0u8);
	let mut v = SharedVec::from_vec((0..10).map(|_| Arc::clone(&probe)).collect::<Vec<_>>());
	v.freeze();
	let mut s = v.slice(2, 5);
	s.make_mut();
	assert_eq!(Arc::strong_count(&probe), 14);
	drop(v);
	assert_eq!(Arc::strong_count(&probe), 4, "dropping the parent must free its storage once the slice has thawed");
	drop(s);
	assert_eq!(Arc::strong_count(&probe), 1);
}
