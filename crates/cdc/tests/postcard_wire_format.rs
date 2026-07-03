// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use postcard::{from_bytes, to_stdvec};
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_value::util::hex;

#[test]
fn empty_key_roundtrip() {
	let key = EncodedKey::new(Vec::<u8>::new());
	let bytes = to_stdvec(&key).unwrap();
	assert_eq!(hex::encode(&bytes), "00");

	let restored: EncodedKey = from_bytes(&bytes).unwrap();
	assert_eq!(restored.as_slice(), key.as_slice());
}

#[test]
fn inline_key_roundtrip_small() {
	let key = EncodedKey::new(vec![0x01, 0x02, 0x03, 0xff]);
	let bytes = to_stdvec(&key).unwrap();
	assert_eq!(hex::encode(&bytes), "04010203ff");

	let restored: EncodedKey = from_bytes(&bytes).unwrap();
	assert_eq!(restored.as_slice(), key.as_slice());
	assert!(matches!(restored, EncodedKey::Inline { .. }));
}

#[test]
fn inline_key_roundtrip_at_capacity() {
	let bytes_payload: Vec<u8> = (0u8..62).collect();
	let key = EncodedKey::new(bytes_payload.clone());
	assert!(matches!(key, EncodedKey::Inline { .. }));

	let wire = to_stdvec(&key).unwrap();
	assert_eq!(wire[0], 62, "expected varint length prefix of 62");
	assert_eq!(&wire[1..], bytes_payload.as_slice());

	let restored: EncodedKey = from_bytes(&wire).unwrap();
	assert_eq!(restored.as_slice(), key.as_slice());
	assert!(matches!(restored, EncodedKey::Inline { .. }));
}

#[test]
fn heap_key_roundtrip_just_over_capacity() {
	let bytes_payload: Vec<u8> = (0u8..63).collect();
	let key = EncodedKey::new(bytes_payload.clone());
	assert!(matches!(key, EncodedKey::Heap(_)));

	let wire = to_stdvec(&key).unwrap();
	assert_eq!(wire[0], 63, "expected varint length prefix of 63");
	assert_eq!(&wire[1..], bytes_payload.as_slice());

	let restored: EncodedKey = from_bytes(&wire).unwrap();
	assert_eq!(restored.as_slice(), key.as_slice());
	assert!(matches!(restored, EncodedKey::Heap(_)));
}

#[test]
fn heap_key_roundtrip_large() {
	let bytes_payload: Vec<u8> = (0u16..1024).map(|n| (n & 0xff) as u8).collect();
	let key = EncodedKey::new(bytes_payload.clone());
	assert!(matches!(key, EncodedKey::Heap(_)));

	let wire = to_stdvec(&key).unwrap();
	let restored: EncodedKey = from_bytes(&wire).unwrap();
	assert_eq!(restored.as_slice(), key.as_slice());
	assert!(matches!(restored, EncodedKey::Heap(_)));
}

#[test]
fn variant_invariant_inline_eq_heap_with_same_bytes() {
	let payload = vec![0xaa, 0xbb, 0xcc];
	let inline = EncodedKey::new(payload.clone());
	let heap = EncodedKey::Heap(payload);
	assert_eq!(inline, heap, "Inline and Heap with the same bytes must compare equal");
	assert_eq!(inline.cmp(&heap), std::cmp::Ordering::Equal);

	let inline_wire = to_stdvec(&inline).unwrap();
	let heap_wire = to_stdvec(&heap).unwrap();
	assert_eq!(inline_wire, heap_wire, "Inline and Heap must serialize to identical bytes");
}

#[test]
fn variant_independent_hash() {
	use std::{collections::hash_map::DefaultHasher, hash::Hasher};

	let payload = vec![0x10, 0x20, 0x30, 0x40];
	let inline = EncodedKey::new(payload.clone());
	let heap = EncodedKey::Heap(payload);

	let mut h1 = DefaultHasher::new();
	std::hash::Hash::hash(&inline, &mut h1);
	let mut h2 = DefaultHasher::new();
	std::hash::Hash::hash(&heap, &mut h2);

	assert_eq!(h1.finish(), h2.finish(), "Hash must depend only on bytes, not on variant");
}
