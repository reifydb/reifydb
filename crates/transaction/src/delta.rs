// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{
	BTreeMap, BTreeSet,
	btree_map::Entry::{Occupied, Vacant},
};

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	delta::{Delta, RemoveAnnounce},
	key::any::TaggedKey,
};

#[derive(Debug, Clone)]
enum OptimizedDeltaState {
	Set {
		bytes: EncodedBytes,
	},

	Remove {
		announce: RemoveAnnounce,
	},

	Cancelled,
}

pub fn optimize_deltas(deltas: impl IntoIterator<Item = Delta>, preexisting_keys: &BTreeSet<TaggedKey>) -> Vec<Delta> {
	let deltas: Vec<Delta> = deltas.into_iter().collect();
	let collided = {
		let mut seen: BTreeSet<&TaggedKey> = BTreeSet::new();
		!deltas.iter().all(|delta| seen.insert(delta.key()))
	};
	if !collided {
		return deltas;
	}
	optimize_collided(deltas, preexisting_keys)
}

fn optimize_collided(deltas: Vec<Delta>, preexisting_keys: &BTreeSet<TaggedKey>) -> Vec<Delta> {
	let mut key_states: BTreeMap<TaggedKey, (OptimizedDeltaState, usize)> = BTreeMap::new();

	for (idx, delta) in deltas.into_iter().enumerate() {
		match delta {
			Delta::Set {
				key,
				bytes,
			} => {
				let entry = key_states.entry(key);
				match entry {
					Occupied(mut occ) => {
						let (state, _) = occ.get_mut();
						match state {
							OptimizedDeltaState::Set {
								bytes: old_bytes,
							} => {
								*old_bytes = bytes;
							}
							OptimizedDeltaState::Remove {
								..
							} => {
								*state = OptimizedDeltaState::Set {
									bytes,
								};
							}
							OptimizedDeltaState::Cancelled => {
								*state = OptimizedDeltaState::Set {
									bytes,
								};
							}
						}
					}
					Vacant(vac) => {
						vac.insert((
							OptimizedDeltaState::Set {
								bytes,
							},
							idx,
						));
					}
				}
			}
			Delta::Remove {
				key,
				announce,
			} => {
				let preexisting = preexisting_keys.contains(&key);
				let entry = key_states.entry(key);
				match entry {
					Occupied(mut occ) => {
						let (state, _) = occ.get_mut();
						match state {
							OptimizedDeltaState::Set {
								..
							} => {
								if preexisting {
									*state = OptimizedDeltaState::Remove {
										announce,
									};
								} else {
									*state = OptimizedDeltaState::Cancelled;
								}
							}
							OptimizedDeltaState::Remove {
								..
							} => {}
							OptimizedDeltaState::Cancelled => {
								*state = OptimizedDeltaState::Remove {
									announce,
								};
							}
						}
					}
					Vacant(vac) => {
						vac.insert((
							OptimizedDeltaState::Remove {
								announce,
							},
							idx,
						));
					}
				}
			}
		}
	}

	let mut result: Vec<(usize, Delta)> = Vec::new();

	for (key, (state, idx)) in key_states {
		match state {
			OptimizedDeltaState::Set {
				bytes,
			} => {
				result.push((
					idx,
					Delta::Set {
						key,
						bytes,
					},
				));
			}
			OptimizedDeltaState::Remove {
				announce,
			} => {
				result.push((
					idx,
					Delta::Remove {
						key,
						announce,
					},
				));
			}
			OptimizedDeltaState::Cancelled => {}
		}
	}

	result.sort_by_key(|(idx, _)| *idx);

	result.into_iter().map(|(_, delta)| delta).collect()
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{interface::catalog::id::QueueId, key::queue::QueueDeduplicationKey};
	use reifydb_value::util::cowvec::CowVec;

	use super::*;

	fn make_key(s: &str) -> TaggedKey {
		QueueDeduplicationKey::new(QueueId(1), s.as_bytes().iter().map(|b| !b).collect::<Vec<u8>>()).into()
	}

	fn make_bytes(s: &str) -> EncodedBytes {
		EncodedBytes(CowVec::new(s.as_bytes().to_vec()))
	}

	#[test]
	fn the_distinct_key_fast_path_agrees_with_the_collision_path() {
		// the fast path returns the input untouched whenever no key repeats, so it must produce exactly what
		// the BTreeMap pass produces for the same input; a divergence here silently rewrites a commit
		let distinct = vec![
			Delta::Set {
				key: make_key("key_a"),
				bytes: make_bytes("a"),
			},
			Delta::Set {
				key: make_key("key_b"),
				bytes: make_bytes("b"),
			},
			Delta::remove_announced(make_key("key_c"), make_bytes("c")),
		];

		let fast = optimize_deltas(distinct.clone(), &BTreeSet::new());
		let collided = optimize_collided(distinct.clone(), &BTreeSet::new());

		assert_eq!(fast.len(), distinct.len(), "a run of distinct keys must keep every delta");
		assert_eq!(
			fast.iter().map(|delta| delta.key().clone()).collect::<Vec<_>>(),
			collided.iter().map(|delta| delta.key().clone()).collect::<Vec<_>>(),
			"both paths must emit the same keys in the same order"
		);
	}

	#[test]
	fn a_repeated_key_still_reaches_the_collision_path() {
		// the fast path must not swallow a duplicate: two sets of one key collapse to the later value
		let repeated = vec![
			Delta::Set {
				key: make_key("key_a"),
				bytes: make_bytes("first"),
			},
			Delta::Set {
				key: make_key("key_a"),
				bytes: make_bytes("second"),
			},
		];

		let optimized = optimize_deltas(repeated, &BTreeSet::new());

		assert_eq!(optimized.len(), 1, "a repeated key must collapse to a single delta");
		match &optimized[0] {
			Delta::Set {
				bytes,
				..
			} => assert_eq!(bytes.0.as_ref(), b"second", "the later write must win"),
			other => panic!("expected a set, got {other:?}"),
		}
	}

	#[test]
	fn delta_log_sourcing_cancels_an_insert_delete_pair() {
		// The two sourcing paths must eventually agree: delta-log cancels the pair, pending-writes emits a
		// tombstone.
		let from_delta_log = vec![
			Delta::Set {
				key: make_key("key_a"),
				bytes: make_bytes("value1"),
			},
			Delta::remove_announced(make_key("key_a"), make_bytes("value1")),
		];

		let optimized = optimize_deltas(from_delta_log, &BTreeSet::new());

		assert!(optimized.is_empty(), "the delta-log path sees both writes and cancels them");
	}

	#[test]
	fn pending_writes_sourcing_emits_a_tombstone_for_the_same_transaction() {
		let from_pending_writes = vec![Delta::remove_announced(make_key("key_a"), make_bytes("value1"))];

		let optimized = optimize_deltas(from_pending_writes, &BTreeSet::new());

		assert_eq!(
			optimized.len(),
			1,
			"the pending-writes path only retains the latest write per key, so the Set is gone before \
			 optimize_deltas runs and the Remove survives as a tombstone"
		);
		assert!(matches!(optimized[0], Delta::Remove { .. }));
	}

	#[test]
	fn test_insert_delete_cancellation() {
		let deltas = vec![
			Delta::Set {
				key: make_key("key_a"),
				bytes: make_bytes("value1"),
			},
			Delta::remove_announced(make_key("key_a"), make_bytes("value1")),
		];

		let optimized = optimize_deltas(deltas, &BTreeSet::new());

		assert_eq!(optimized.len(), 0);
	}

	#[test]
	fn test_update_delete_keeps_tombstone() {
		let deltas = vec![
			Delta::Set {
				key: make_key("key_a"),
				bytes: make_bytes("value1"),
			},
			Delta::remove_announced(make_key("key_a"), make_bytes("value1")),
		];

		let mut preexisting = BTreeSet::new();
		preexisting.insert(make_key("key_a"));
		let optimized = optimize_deltas(deltas, &preexisting);

		// Dropping the tombstone here would leave the prior committed version visible.
		assert_eq!(optimized.len(), 1);
		match &optimized[0] {
			Delta::Remove {
				key,
				announce: RemoveAnnounce::Announced {
					pre,
				},
			} => {
				assert_eq!(key, &make_key("key_a"));
				assert_eq!(
					pre.0.as_slice(),
					b"value1",
					"coalescing must carry the pre-image through, or CDC announces a delete with no before-image"
				);
			}
			other => panic!("Expected an announced Delta::Remove, got {other:?}"),
		}
	}

	#[test]
	fn test_update_silent_remove_keeps_tombstone_and_stays_silent() {
		// Silence controls only whether CDC hears about the removal, never whether the bytes is
		// gone; collapsing it away would leave the prior version readable.
		let deltas = vec![
			Delta::Set {
				key: make_key("key_a"),
				bytes: make_bytes("value1"),
			},
			Delta::remove_silent(make_key("key_a")),
		];

		let mut preexisting = BTreeSet::new();
		preexisting.insert(make_key("key_a"));
		let optimized = optimize_deltas(deltas, &preexisting);

		assert_eq!(optimized.len(), 1);
		match &optimized[0] {
			Delta::Remove {
				key,
				announce,
			} => {
				assert_eq!(key, &make_key("key_a"));
				assert_eq!(
					*announce,
					RemoveAnnounce::Silent,
					"coalescing must not promote a silent removal into an announced one"
				);
			}
			other => panic!("Expected Delta::Remove, got {other:?}"),
		}
	}

	#[test]
	fn test_update_coalescing() {
		let deltas = vec![
			Delta::Set {
				key: make_key("key_a"),
				bytes: make_bytes("value1"),
			},
			Delta::Set {
				key: make_key("key_a"),
				bytes: make_bytes("value2"),
			},
			Delta::Set {
				key: make_key("key_a"),
				bytes: make_bytes("value3"),
			},
		];

		let optimized = optimize_deltas(deltas, &BTreeSet::new());

		assert_eq!(optimized.len(), 1);
		match &optimized[0] {
			Delta::Set {
				key,
				bytes,
			} => {
				assert_eq!(key, &make_key("key_a"));
				assert_eq!(bytes.0.as_slice(), b"value3");
			}
			_ => panic!("Expected Set delta"),
		}
	}

	#[test]
	fn test_insert_update_delete() {
		let deltas = vec![
			Delta::Set {
				key: make_key("key_a"),
				bytes: make_bytes("value1"),
			},
			Delta::Set {
				key: make_key("key_a"),
				bytes: make_bytes("value2"),
			},
			Delta::remove_announced(make_key("key_a"), make_bytes("value2")),
		];

		let optimized = optimize_deltas(deltas, &BTreeSet::new());

		assert_eq!(optimized.len(), 0);
	}

	#[test]
	fn test_multiple_keys() {
		let deltas = vec![
			Delta::Set {
				key: make_key("key_a"),
				bytes: make_bytes("value1"),
			},
			Delta::Set {
				key: make_key("key_b"),
				bytes: make_bytes("value2"),
			},
			Delta::remove_announced(make_key("key_a"), make_bytes("value1")),
			Delta::Set {
				key: make_key("key_c"),
				bytes: make_bytes("value3"),
			},
		];

		let optimized = optimize_deltas(deltas, &BTreeSet::new());

		// key_a cancels; key_b and key_c survive.
		assert_eq!(optimized.len(), 2);
	}
}
