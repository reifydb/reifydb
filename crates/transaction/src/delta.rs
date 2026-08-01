// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashSet;

use indexmap::{
	IndexMap,
	map::Entry::{Occupied, Vacant},
};
use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use reifydb_core::delta::{Delta, RemoveAnnounce};

#[derive(Debug, Clone)]
enum OptimizedDeltaState {
	Set {
		row: EncodedRow,
	},

	Remove {
		announce: RemoveAnnounce,
	},

	Cancelled,
}

pub fn optimize_deltas(deltas: impl IntoIterator<Item = Delta>, preexisting_keys: &HashSet<EncodedKey>) -> Vec<Delta> {
	let mut key_states: IndexMap<EncodedKey, (OptimizedDeltaState, usize)> = IndexMap::new();

	for (idx, delta) in deltas.into_iter().enumerate() {
		match delta {
			Delta::Set {
				key,
				row,
			} => {
				let entry = key_states.entry(key);
				match entry {
					Occupied(mut occ) => {
						let (state, _) = occ.get_mut();
						match state {
							OptimizedDeltaState::Set {
								row: old_row,
							} => {
								*old_row = row;
							}
							OptimizedDeltaState::Remove {
								..
							} => {
								*state = OptimizedDeltaState::Set {
									row,
								};
							}
							OptimizedDeltaState::Cancelled => {
								*state = OptimizedDeltaState::Set {
									row,
								};
							}
						}
					}
					Vacant(vac) => {
						vac.insert((
							OptimizedDeltaState::Set {
								row,
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
				row,
			} => {
				result.push((
					idx,
					Delta::Set {
						key,
						row,
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
	use reifydb_value::util::cowvec::CowVec;

	use super::*;

	fn make_key(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes())
	}

	fn make_row(s: &str) -> EncodedRow {
		EncodedRow(CowVec::new(s.as_bytes().to_vec()))
	}

	#[test]
	fn delta_log_sourcing_cancels_an_insert_delete_pair() {
		// Pinned divergence: the primary path retains both writes and cancels them, while the
		// replica path (next test) sees only the Remove and emits a tombstone for the same
		// transaction. The two must eventually agree, so any unification has to be deliberate.
		let from_delta_log = vec![
			Delta::Set {
				key: make_key("key_a"),
				row: make_row("value1"),
			},
			Delta::remove_announced(make_key("key_a"), make_row("value1")),
		];

		let optimized = optimize_deltas(from_delta_log, &HashSet::new());

		assert!(optimized.is_empty(), "the primary path sees both writes and cancels them");
	}

	#[test]
	fn pending_writes_sourcing_emits_a_tombstone_for_the_same_transaction() {
		let from_pending_writes = vec![Delta::remove_announced(make_key("key_a"), make_row("value1"))];

		let optimized = optimize_deltas(from_pending_writes, &HashSet::new());

		assert_eq!(
			optimized.len(),
			1,
			"the replica path only retains the latest write per key, so the Set is gone before \
			 optimize_deltas runs and the Remove survives as a tombstone"
		);
		assert!(matches!(optimized[0], Delta::Remove { .. }));
	}

	#[test]
	fn test_insert_delete_cancellation() {
		let deltas = vec![
			Delta::Set {
				key: make_key("key_a"),
				row: make_row("value1"),
			},
			Delta::remove_announced(make_key("key_a"), make_row("value1")),
		];

		let optimized = optimize_deltas(deltas, &HashSet::new());

		assert_eq!(optimized.len(), 0);
	}

	#[test]
	fn test_update_delete_keeps_tombstone() {
		let deltas = vec![
			Delta::Set {
				key: make_key("key_a"),
				row: make_row("value1"),
			},
			Delta::remove_announced(make_key("key_a"), make_row("value1")),
		];

		let mut preexisting = HashSet::new();
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
				assert_eq!(key.as_ref(), b"key_a");
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
		// Silence controls only whether CDC hears about the removal, never whether the row is
		// gone; collapsing it away would leave the prior version readable.
		let deltas = vec![
			Delta::Set {
				key: make_key("key_a"),
				row: make_row("value1"),
			},
			Delta::remove_silent(make_key("key_a")),
		];

		let mut preexisting = HashSet::new();
		preexisting.insert(make_key("key_a"));
		let optimized = optimize_deltas(deltas, &preexisting);

		assert_eq!(optimized.len(), 1);
		match &optimized[0] {
			Delta::Remove {
				key,
				announce,
			} => {
				assert_eq!(key.as_ref(), b"key_a");
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
				row: make_row("value1"),
			},
			Delta::Set {
				key: make_key("key_a"),
				row: make_row("value2"),
			},
			Delta::Set {
				key: make_key("key_a"),
				row: make_row("value3"),
			},
		];

		let optimized = optimize_deltas(deltas, &HashSet::new());

		assert_eq!(optimized.len(), 1);
		match &optimized[0] {
			Delta::Set {
				key,
				row,
			} => {
				assert_eq!(key.as_ref(), b"key_a");
				assert_eq!(row.0.as_slice(), b"value3");
			}
			_ => panic!("Expected Set delta"),
		}
	}

	#[test]
	fn test_insert_update_delete() {
		let deltas = vec![
			Delta::Set {
				key: make_key("key_a"),
				row: make_row("value1"),
			},
			Delta::Set {
				key: make_key("key_a"),
				row: make_row("value2"),
			},
			Delta::remove_announced(make_key("key_a"), make_row("value2")),
		];

		let optimized = optimize_deltas(deltas, &HashSet::new());

		assert_eq!(optimized.len(), 0);
	}

	#[test]
	fn test_multiple_keys() {
		let deltas = vec![
			Delta::Set {
				key: make_key("key_a"),
				row: make_row("value1"),
			},
			Delta::Set {
				key: make_key("key_b"),
				row: make_row("value2"),
			},
			Delta::remove_announced(make_key("key_a"), make_row("value1")),
			Delta::Set {
				key: make_key("key_c"),
				row: make_row("value3"),
			},
		];

		let optimized = optimize_deltas(deltas, &HashSet::new());

		// key_a cancels; key_b and key_c survive.
		assert_eq!(optimized.len(), 2);
	}
}
