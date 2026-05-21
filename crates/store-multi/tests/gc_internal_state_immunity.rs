// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Locks down the invariant that the operator TTL GC never expires entries
//! written under `FlowNodeInternalStateKey`. The scanner at
//! `crates/store-multi/src/gc/operator/scanner.rs` ranges over
//! `FlowNodeStateKey::node_range(node_id)` ONLY; this file asserts that
//! contract structurally (the test passes regardless of the inner-tag byte
//! used by `RowNumberProvider`, windowed-driver meta, or `GateOperator`).

use std::collections::HashMap;

use reifydb_core::{
	common::CommitVersion,
	encoded::{key::EncodedKey, row::SHAPE_HEADER_SIZE},
	interface::{catalog::flow::FlowNodeId, store::EntryKind},
	key::{flow_node_internal_state::FlowNodeInternalStateKey, flow_node_state::FlowNodeStateKey},
	row::{Ttl, TtlAnchor, TtlCleanupMode},
};
use reifydb_store_multi::{
	buffer::tier::MultiBufferTier,
	gc::operator::{
		OperatorScanStats,
		scanner::{
			ExpiredOperatorState, drop_expired_operator_keys, scan_operator_by_created_at,
			scan_operator_by_updated_at,
		},
	},
	tier::{RangeCursor, TierStorage},
};
use reifydb_type::util::cowvec::CowVec;

const NODE: FlowNodeId = FlowNodeId(1);

fn build_row(payload: &[u8], created_at: u64, updated_at: u64) -> CowVec<u8> {
	let mut buf = vec![0u8; SHAPE_HEADER_SIZE + payload.len()];
	buf[8..16].copy_from_slice(&created_at.to_le_bytes());
	buf[16..24].copy_from_slice(&updated_at.to_le_bytes());
	buf[SHAPE_HEADER_SIZE..].copy_from_slice(payload);
	CowVec::new(buf)
}

fn write(storage: &MultiBufferTier, key: EncodedKey, payload: &[u8], created_at: u64) {
	// Scanner asserts both created_at_nanos > 0 and updated_at_nanos > 0
	// as invariants (see scanner.rs:50, 95). Use max(1, created_at) so
	// callers may still pass 0 conceptually but the row survives the
	// invariant check.
	let ts = created_at.max(1);
	let row = build_row(payload, ts, ts);
	let mut batch: HashMap<EntryKind, Vec<(EncodedKey, Option<CowVec<u8>>)>> = HashMap::new();
	batch.insert(EntryKind::Operator(NODE), vec![(key, Some(row))]);
	storage.set(CommitVersion(1), batch).unwrap();
}

/// Returns the payload bytes (the portion after the row's timestamp header)
/// or `None` if the key isn't present. Lets tests assert that the right
/// value survives, not just that *some* value does.
fn read_payload(storage: &MultiBufferTier, key: &EncodedKey) -> Option<Vec<u8>> {
	let bytes = storage.get(EntryKind::Operator(NODE), key, CommitVersion(u64::MAX)).unwrap().value()?;
	Some(bytes[SHAPE_HEADER_SIZE..].to_vec())
}

fn fns_key(inner: Vec<u8>) -> EncodedKey {
	FlowNodeStateKey::encoded(NODE, inner)
}

fn fis_key(inner: Vec<u8>) -> EncodedKey {
	FlowNodeInternalStateKey::encoded(NODE, inner)
}

fn run_scanner_by_created(storage: &MultiBufferTier) -> Vec<ExpiredOperatorState> {
	let ttl = Ttl {
		duration_nanos: 1,
		anchor: TtlAnchor::Created,
		cleanup_mode: TtlCleanupMode::Drop,
	};
	let mut cursor = RangeCursor::new();
	let (expired, _) = scan_operator_by_created_at(storage, NODE, &ttl, 1_000_000_000, 1024, &mut cursor).unwrap();
	expired
}

fn run_scanner_by_updated(storage: &MultiBufferTier) -> Vec<ExpiredOperatorState> {
	let ttl = Ttl {
		duration_nanos: 1,
		anchor: TtlAnchor::Updated,
		cleanup_mode: TtlCleanupMode::Drop,
	};
	let mut cursor = RangeCursor::new();
	let (expired, _) = scan_operator_by_updated_at(storage, NODE, &ttl, 1_000_000_000, 1024, &mut cursor).unwrap();
	expired
}

fn drop_keys(storage: &MultiBufferTier, expired: &[ExpiredOperatorState]) {
	let mut stats = OperatorScanStats::default();
	drop_expired_operator_keys(storage, expired, &mut stats).unwrap();
}

#[test]
fn flow_node_state_is_expired() {
	let storage = MultiBufferTier::memory();
	let key = fns_key(vec![0x01]);
	let payload = b"fns_payload";
	write(&storage, key.clone(), payload, 0);

	// Sanity: the entry is present and readable before the GC fires.
	assert_eq!(read_payload(&storage, &key), Some(payload.to_vec()));

	let expired = run_scanner_by_created(&storage);
	assert_eq!(expired.len(), 1, "FNS entry must be reported as expired under aggressive TTL");
	assert_eq!(expired[0].key, key);

	drop_keys(&storage, &expired);
	assert_eq!(read_payload(&storage, &key), None, "FNS entry must be gone after drop_expired_operator_keys");
}

#[test]
fn row_number_counter_is_immune() {
	let storage = MultiBufferTier::memory();
	let fns = fns_key(vec![0x01]);
	let counter = fis_key(vec![FlowNodeInternalStateKey::ROW_NUMBER_COUNTER_TAG]);
	let fns_payload = b"fns_data";
	let counter_payload = b"counter_value_42";
	write(&storage, fns.clone(), fns_payload, 0);
	write(&storage, counter.clone(), counter_payload, 0);

	let expired = run_scanner_by_created(&storage);
	assert_eq!(expired.len(), 1, "scanner must report FNS only");
	assert_eq!(expired[0].key, fns);

	drop_keys(&storage, &expired);
	assert_eq!(read_payload(&storage, &fns), None);
	assert_eq!(
		read_payload(&storage, &counter),
		Some(counter_payload.to_vec()),
		"RowNumberProvider counter value must survive TTL GC byte-for-byte"
	);
}

#[test]
fn row_number_mapping_is_immune() {
	let storage = MultiBufferTier::memory();
	let fns = fns_key(vec![0x01]);
	// Inner bytes mirror what `make_map_key` writes for user key b"k1":
	// [tag, escaped_user_bytes..., 0xff, 0xff]
	let mut mapping_inner = vec![FlowNodeInternalStateKey::ROW_NUMBER_MAPPING_TAG];
	mapping_inner.extend_from_slice(b"k1");
	mapping_inner.extend_from_slice(&[0xff, 0xff]);
	let mapping = fis_key(mapping_inner);
	let fns_payload = b"fns_data";
	let mapping_payload = b"mapping_to_row_7";
	write(&storage, fns.clone(), fns_payload, 0);
	write(&storage, mapping.clone(), mapping_payload, 0);

	let expired = run_scanner_by_created(&storage);
	assert_eq!(expired.len(), 1);
	assert_eq!(expired[0].key, fns);

	drop_keys(&storage, &expired);
	assert_eq!(
		read_payload(&storage, &mapping),
		Some(mapping_payload.to_vec()),
		"RowNumberProvider mapping value must survive TTL GC byte-for-byte"
	);
}

#[test]
fn window_meta_is_immune() {
	let storage = MultiBufferTier::memory();
	let fns = fns_key(vec![0x01]);
	let mut meta_inner = vec![FlowNodeInternalStateKey::WINDOW_META_TAG];
	meta_inner.extend_from_slice(b"some_group_encoding");
	let meta = fis_key(meta_inner);
	let fns_payload = b"fns_data";
	let meta_payload = b"high_water=12345";
	write(&storage, fns.clone(), fns_payload, 0);
	write(&storage, meta.clone(), meta_payload, 0);

	let expired = run_scanner_by_created(&storage);
	assert_eq!(expired.len(), 1);
	assert_eq!(expired[0].key, fns);

	drop_keys(&storage, &expired);
	assert_eq!(
		read_payload(&storage, &meta),
		Some(meta_payload.to_vec()),
		"windowed-driver meta value must survive TTL GC byte-for-byte"
	);
}

#[test]
fn gate_visibility_is_immune() {
	let storage = MultiBufferTier::memory();
	let fns = fns_key(vec![0x01]);
	let mut gate_inner = vec![FlowNodeInternalStateKey::GATE_VISIBILITY_TAG];
	gate_inner.extend_from_slice(&42u64.to_be_bytes());
	let gate = fis_key(gate_inner);
	let fns_payload = b"fns_data";
	// Matches the production `VISIBLE_MARKER` in gate.rs: `vec![1]`.
	let gate_payload = b"\x01";
	write(&storage, fns.clone(), fns_payload, 0);
	write(&storage, gate.clone(), gate_payload, 0);

	let expired = run_scanner_by_created(&storage);
	assert_eq!(expired.len(), 1);
	assert_eq!(expired[0].key, fns);

	drop_keys(&storage, &expired);
	assert_eq!(
		read_payload(&storage, &gate),
		Some(gate_payload.to_vec()),
		"gate visibility marker value must survive TTL GC byte-for-byte"
	);
}

#[test]
fn any_internal_state_entry_is_immune_regardless_of_tag() {
	// FIS immunity is structural - the scanner doesn't range over the
	// FlowNodeInternalState KeyKind at all - not tag-specific. Even a
	// rogue FIS entry with no recognized inner-tag survives.
	let storage = MultiBufferTier::memory();
	let fns = fns_key(vec![0x01]);
	let rogue = fis_key(vec![0xAA, 0xBB, 0xCC]);
	let fns_payload = b"fns_data";
	let rogue_payload = b"unrecognized_tag_payload";
	write(&storage, fns.clone(), fns_payload, 0);
	write(&storage, rogue.clone(), rogue_payload, 0);

	let expired = run_scanner_by_created(&storage);
	assert_eq!(expired.len(), 1);
	assert_eq!(expired[0].key, fns);

	drop_keys(&storage, &expired);
	assert_eq!(
		read_payload(&storage, &rogue),
		Some(rogue_payload.to_vec()),
		"any FIS entry survives TTL GC regardless of inner-tag"
	);
}

#[test]
fn scan_operator_by_updated_at_is_also_immune() {
	// Mirror of `row_number_counter_is_immune` against the updated-at
	// scanner variant. Both code paths in scanner.rs share the same
	// range scope (FlowNodeStateKey only), so both must respect the
	// same immunity boundary.
	let storage = MultiBufferTier::memory();
	let fns = fns_key(vec![0x01]);
	let counter = fis_key(vec![FlowNodeInternalStateKey::ROW_NUMBER_COUNTER_TAG]);
	let fns_payload = b"fns_data";
	let counter_payload = b"counter_value_99";
	write(&storage, fns.clone(), fns_payload, 0);
	write(&storage, counter.clone(), counter_payload, 0);

	let expired = run_scanner_by_updated(&storage);
	assert_eq!(expired.len(), 1);
	assert_eq!(expired[0].key, fns);

	drop_keys(&storage, &expired);
	assert_eq!(read_payload(&storage, &fns), None);
	assert_eq!(read_payload(&storage, &counter), Some(counter_payload.to_vec()));
}

#[test]
fn mixed_batch_only_drops_flow_node_state() {
	// Production smoke: one FNS entry + one FIS entry per recognized tag,
	// each carrying a DISTINCT payload so the assertions verify the
	// scanner preserves the RIGHT entry with its RIGHT bytes, not just
	// that SOME entry survives.
	let storage = MultiBufferTier::memory();
	let fns = fns_key(vec![0x01]);
	let counter = fis_key(vec![FlowNodeInternalStateKey::ROW_NUMBER_COUNTER_TAG]);
	let mut mapping_inner = vec![FlowNodeInternalStateKey::ROW_NUMBER_MAPPING_TAG];
	mapping_inner.extend_from_slice(b"some_user_key");
	mapping_inner.extend_from_slice(&[0xff, 0xff]);
	let mapping = fis_key(mapping_inner);
	let mut meta_inner = vec![FlowNodeInternalStateKey::WINDOW_META_TAG];
	meta_inner.extend_from_slice(b"group_xyz");
	let meta = fis_key(meta_inner);
	let mut gate_inner = vec![FlowNodeInternalStateKey::GATE_VISIBILITY_TAG];
	gate_inner.extend_from_slice(&7u64.to_be_bytes());
	let gate = fis_key(gate_inner);

	let fns_payload = b"FNS-value";
	let counter_payload = b"C-value";
	let mapping_payload = b"M-value";
	let meta_payload = b"W-value";
	let gate_payload = b"G-value";

	write(&storage, fns.clone(), fns_payload, 0);
	write(&storage, counter.clone(), counter_payload, 0);
	write(&storage, mapping.clone(), mapping_payload, 0);
	write(&storage, meta.clone(), meta_payload, 0);
	write(&storage, gate.clone(), gate_payload, 0);

	let expired = run_scanner_by_created(&storage);
	assert_eq!(expired.len(), 1, "scanner must report exactly one expired (the FNS entry)");
	assert_eq!(expired[0].key, fns);

	drop_keys(&storage, &expired);
	assert_eq!(read_payload(&storage, &fns), None);
	assert_eq!(read_payload(&storage, &counter), Some(counter_payload.to_vec()));
	assert_eq!(read_payload(&storage, &mapping), Some(mapping_payload.to_vec()));
	assert_eq!(read_payload(&storage, &meta), Some(meta_payload.to_vec()));
	assert_eq!(read_payload(&storage, &gate), Some(gate_payload.to_vec()));
}
