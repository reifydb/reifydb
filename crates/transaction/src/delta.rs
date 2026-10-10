// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{
	BTreeMap, BTreeSet,
	btree_map::Entry::{Occupied, Vacant},
};

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	delta::{Delta, RemoveAnnounce, SetPre},
	key::any::TaggedKey,
};

#[derive(Debug, Clone)]
enum OptimizedDeltaState {
	Set {
		bytes: EncodedBytes,
		pre: SetPre,
	},

	Remove {
		announce: RemoveAnnounce,
	},

	Cancelled,
}

pub fn optimize_deltas(
	deltas: impl IntoIterator<Item = Delta>,
	committed: &BTreeMap<TaggedKey, EncodedBytes>,
) -> Vec<Delta> {
	let deltas: Vec<Delta> = deltas.into_iter().collect();
	let collided = {
		let mut seen: BTreeSet<&TaggedKey> = BTreeSet::new();
		!deltas.iter().all(|delta| seen.insert(delta.key()))
	};
	if !collided {
		return deltas;
	}
	optimize_collided(deltas, committed)
}

fn with_pre(announce: RemoveAnnounce, pre: EncodedBytes) -> RemoveAnnounce {
	match announce {
		RemoveAnnounce::Silent => RemoveAnnounce::Silent,
		RemoveAnnounce::Announced {
			..
		} => RemoveAnnounce::Announced {
			pre,
		},
		RemoveAnnounce::Unobserved {
			..
		} => RemoveAnnounce::Unobserved {
			pre,
		},
	}
}

fn optimize_collided(deltas: Vec<Delta>, committed: &BTreeMap<TaggedKey, EncodedBytes>) -> Vec<Delta> {
	let mut key_states: BTreeMap<TaggedKey, (OptimizedDeltaState, usize, SetPre)> = BTreeMap::new();

	for (idx, delta) in deltas.into_iter().enumerate() {
		match delta {
			Delta::Set {
				key,
				bytes,
				pre,
			} => {
				let entry = key_states.entry(key);
				match entry {
					Occupied(mut occ) => {
						let (state, _, known) = occ.get_mut();
						match state {
							OptimizedDeltaState::Set {
								bytes: old_bytes,
								..
							} => {
								*old_bytes = bytes;
							}
							OptimizedDeltaState::Remove {
								..
							} => {
								*state = OptimizedDeltaState::Set {
									bytes,
									pre: SetPre::Unknown,
								};
							}
							OptimizedDeltaState::Cancelled => {
								*state = OptimizedDeltaState::Set {
									bytes,
									pre: known.clone(),
								};
							}
						}
					}
					Vacant(vac) => {
						vac.insert((
							OptimizedDeltaState::Set {
								bytes,
								pre: pre.clone(),
							},
							idx,
							pre,
						));
					}
				}
			}
			Delta::Remove {
				key,
				announce,
			} => {
				let committed_pre = committed.get(&key);
				let entry = key_states.entry(key);
				match entry {
					Occupied(mut occ) => {
						let (state, _, known) = occ.get_mut();
						match state {
							OptimizedDeltaState::Set {
								..
							} => {
								let removed = match known {
									SetPre::Absent => None,
									SetPre::Present(pre) => {
										Some(with_pre(announce, pre.clone()))
									}
									SetPre::Unknown => committed_pre.map(|pre| {
										with_pre(announce, pre.clone())
									}),
								};
								*state = match removed {
									Some(announce) => OptimizedDeltaState::Remove {
										announce,
									},
									None => OptimizedDeltaState::Cancelled,
								};
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
							SetPre::Unknown,
						));
					}
				}
			}
		}
	}

	let mut result: Vec<(usize, Delta)> = Vec::new();

	for (key, (state, idx, _)) in key_states {
		match state {
			OptimizedDeltaState::Set {
				bytes,
				pre,
			} => {
				result.push((
					idx,
					Delta::Set {
						key,
						bytes,
						pre,
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
				pre: SetPre::Unknown,
			},
			Delta::Set {
				key: make_key("key_b"),
				bytes: make_bytes("b"),
				pre: SetPre::Unknown,
			},
			Delta::remove_announced(make_key("key_c"), make_bytes("c")),
		];

		let fast = optimize_deltas(distinct.clone(), &BTreeMap::new());
		let collided = optimize_collided(distinct.clone(), &BTreeMap::new());

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
				pre: SetPre::Unknown,
			},
			Delta::Set {
				key: make_key("key_a"),
				bytes: make_bytes("second"),
				pre: SetPre::Unknown,
			},
		];

		let optimized = optimize_deltas(repeated, &BTreeMap::new());

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
				pre: SetPre::Unknown,
			},
			Delta::remove_announced(make_key("key_a"), make_bytes("value1")),
		];

		let optimized = optimize_deltas(from_delta_log, &BTreeMap::new());

		assert!(optimized.is_empty(), "the delta-log path sees both writes and cancels them");
	}

	#[test]
	fn pending_writes_sourcing_emits_a_tombstone_for_the_same_transaction() {
		let from_pending_writes = vec![Delta::remove_announced(make_key("key_a"), make_bytes("value1"))];

		let optimized = optimize_deltas(from_pending_writes, &BTreeMap::new());

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
				pre: SetPre::Unknown,
			},
			Delta::remove_announced(make_key("key_a"), make_bytes("value1")),
		];

		let optimized = optimize_deltas(deltas, &BTreeMap::new());

		assert_eq!(optimized.len(), 0);
	}

	#[test]
	fn test_update_delete_keeps_tombstone() {
		let deltas = vec![
			Delta::Set {
				key: make_key("key_a"),
				bytes: make_bytes("value1"),
				pre: SetPre::Present(make_bytes("value1")),
			},
			Delta::remove_announced(make_key("key_a"), make_bytes("value1")),
		];

		let optimized = optimize_deltas(deltas, &BTreeMap::new());

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
				pre: SetPre::Present(make_bytes("value1")),
			},
			Delta::remove_silent(make_key("key_a")),
		];

		let optimized = optimize_deltas(deltas, &BTreeMap::new());

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
				pre: SetPre::Unknown,
			},
			Delta::Set {
				key: make_key("key_a"),
				bytes: make_bytes("value2"),
				pre: SetPre::Unknown,
			},
			Delta::Set {
				key: make_key("key_a"),
				bytes: make_bytes("value3"),
				pre: SetPre::Unknown,
			},
		];

		let optimized = optimize_deltas(deltas, &BTreeMap::new());

		assert_eq!(optimized.len(), 1);
		match &optimized[0] {
			Delta::Set {
				key,
				bytes,
				..
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
				pre: SetPre::Unknown,
			},
			Delta::Set {
				key: make_key("key_a"),
				bytes: make_bytes("value2"),
				pre: SetPre::Unknown,
			},
			Delta::remove_announced(make_key("key_a"), make_bytes("value2")),
		];

		let optimized = optimize_deltas(deltas, &BTreeMap::new());

		assert_eq!(optimized.len(), 0);
	}

	#[test]
	fn test_multiple_keys() {
		let deltas = vec![
			Delta::Set {
				key: make_key("key_a"),
				bytes: make_bytes("value1"),
				pre: SetPre::Unknown,
			},
			Delta::Set {
				key: make_key("key_b"),
				bytes: make_bytes("value2"),
				pre: SetPre::Unknown,
			},
			Delta::remove_announced(make_key("key_a"), make_bytes("value1")),
			Delta::Set {
				key: make_key("key_c"),
				bytes: make_bytes("value3"),
				pre: SetPre::Unknown,
			},
		];

		let optimized = optimize_deltas(deltas, &BTreeMap::new());

		// key_a cancels; key_b and key_c survive.
		assert_eq!(optimized.len(), 2);
	}

	fn set(key: &str, bytes: &str, pre: SetPre) -> Delta {
		Delta::Set {
			key: make_key(key),
			bytes: make_bytes(bytes),
			pre,
		}
	}

	fn committed(entries: &[(&str, &str)]) -> BTreeMap<TaggedKey, EncodedBytes> {
		entries.iter().map(|(key, bytes)| (make_key(key), make_bytes(bytes))).collect()
	}

	#[test]
	fn an_unmarked_committed_key_set_then_removed_is_removed_with_the_committed_pre_image() {
		// Cancelling the pair leaves the committed row readable after its writer removed it, and consumers
		// last saw the committed value, so the delete must carry that value and not the overwrite.
		let deltas = vec![
			set("key_a", "overwrite_a", SetPre::Unknown),
			Delta::remove_announced(make_key("key_a"), make_bytes("overwrite_a")),
			set("key_b", "overwrite_b", SetPre::Unknown),
			Delta::remove_unobserved(make_key("key_b"), make_bytes("overwrite_b")),
			set("key_c", "overwrite_c", SetPre::Unknown),
			Delta::remove_silent(make_key("key_c")),
			set("key_d", "fresh_d", SetPre::Unknown),
			Delta::remove_announced(make_key("key_d"), make_bytes("fresh_d")),
		];

		let optimized = optimize_deltas(
			deltas,
			&committed(&[("key_a", "committed_a"), ("key_b", "committed_b"), ("key_c", "committed_c")]),
		);

		assert_eq!(
			optimized,
			vec![
				Delta::remove_announced(make_key("key_a"), make_bytes("committed_a")),
				Delta::remove_unobserved(make_key("key_b"), make_bytes("committed_b")),
				Delta::remove_silent(make_key("key_c")),
			]
		);
	}

	#[test]
	fn a_known_present_tag_wins_over_the_committed_read() {
		// A known tag is the writer's own pre-image; letting a read answer override it would change the
		// pre-image that writer publishes.
		let deltas = vec![
			set("key_a", "overwrite_a", SetPre::Present(make_bytes("overwrite_a"))),
			Delta::remove_announced(make_key("key_a"), make_bytes("overwrite_a")),
		];

		let optimized = optimize_deltas(deltas, &committed(&[("key_a", "committed_a")]));

		assert_eq!(optimized, vec![Delta::remove_announced(make_key("key_a"), make_bytes("overwrite_a"))]);
	}

	#[test]
	fn a_known_pre_image_on_the_first_set_decides_a_set_then_remove_without_the_committed_read() {
		// A tag is what the writer knew before the transaction: absent means the pair never existed outside
		// it, present means a committed row was removed and consumers last saw the tagged bytes.
		let deltas = vec![
			set("key_a", "new_a", SetPre::Absent),
			Delta::remove_announced(make_key("key_a"), make_bytes("new_a")),
			set("key_b", "new_b", SetPre::Present(make_bytes("tagged_b"))),
			Delta::remove_announced(make_key("key_b"), make_bytes("new_b")),
		];

		let optimized =
			optimize_deltas(deltas, &committed(&[("key_a", "committed_a"), ("key_b", "committed_b")]));

		assert_eq!(optimized, vec![Delta::remove_announced(make_key("key_b"), make_bytes("tagged_b"))]);
	}

	#[test]
	fn three_delta_folds_keep_the_knowledge_of_the_first_delta() {
		// The surviving set's tag is what the producer publishes as insert or update, so a fold that loses
		// or invents the first delta's knowledge misclassifies the change.
		let present = || SetPre::Present(make_bytes("tagged"));
		let deltas = vec![
			set("key_a", "a1", SetPre::Absent),
			Delta::remove_announced(make_key("key_a"), make_bytes("a1")),
			set("key_a", "a2", SetPre::Unknown),
			set("key_b", "b1", present()),
			set("key_b", "b2", SetPre::Unknown),
			Delta::remove_announced(make_key("key_b"), make_bytes("b2")),
			Delta::remove_announced(make_key("key_c"), make_bytes("committed_c")),
			set("key_c", "c1", SetPre::Unknown),
			Delta::remove_announced(make_key("key_c"), make_bytes("c1")),
			set("key_d", "d1", present()),
			set("key_d", "d2", SetPre::Unknown),
			Delta::remove_announced(make_key("key_e"), make_bytes("committed_e")),
			set("key_e", "e1", SetPre::Unknown),
			set("key_f", "f1", SetPre::Unknown),
			Delta::remove_announced(make_key("key_f"), make_bytes("f1")),
			set("key_f", "f2", SetPre::Unknown),
		];

		let optimized =
			optimize_deltas(deltas, &committed(&[("key_c", "committed_c"), ("key_f", "committed_f")]));

		assert_eq!(
			optimized,
			vec![
				set("key_a", "a2", SetPre::Absent),
				Delta::remove_announced(make_key("key_b"), make_bytes("tagged")),
				Delta::remove_announced(make_key("key_c"), make_bytes("committed_c")),
				set("key_d", "d2", present()),
				set("key_e", "e1", SetPre::Unknown),
				set("key_f", "f2", SetPre::Unknown),
			]
		);
	}

	#[test]
	fn a_set_after_a_set_then_remove_publishes_an_unknown_pre_image() {
		// After the pair folds to a remove, the first tag no longer says whether the row exists when the
		// later set lands, so the producer must read instead of trusting that tag to pick insert or update.
		let deltas = vec![
			set("key_a", "a1", SetPre::Present(make_bytes("tagged_a"))),
			Delta::remove_announced(make_key("key_a"), make_bytes("a1")),
			set("key_a", "a2", SetPre::Unknown),
		];

		let optimized = optimize_deltas(deltas, &BTreeMap::new());

		assert_eq!(optimized, vec![set("key_a", "a2", SetPre::Unknown)]);
	}
}
