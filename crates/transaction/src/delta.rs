// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashSet;

use indexmap::{
	IndexMap,
	map::Entry::{Occupied, Vacant},
};
use reifydb_core::{
	delta::Delta,
	encoded::{key::EncodedKey, row::EncodedRow},
};
use reifydb_type::util::cowvec::CowVec;

/// Represents the optimized state of a key after all operations in a transaction
#[derive(Debug, Clone)]
enum OptimizedDeltaState {
	/// Key should be set to this value (Insert or Update)
	Set {
		row: EncodedRow,
	},
	/// Key should be unset, preserving the deleted values for CDC/metrics
	Unset {
		row: EncodedRow,
	},
	/// Key should be removed without preserving values
	Remove,
	/// Key operations cancelled out (Insert+Delete), skip entirely
	Cancelled,
}

/// Optimize deltas by applying cancellation and coalescing logic at the delta level.
///
/// `preexisting_keys` lists keys that existed in committed storage before this
/// transaction (populated by Update / Delete operations after they read the prior
/// row). The optimizer uses it to decide whether `Set + Unset` on a key represents
/// a true intra-transaction Insert+Delete (cancellable) or an Update of a prior
/// committed row (cancelling would silently drop the required tombstone). When
/// the key is preexisting, Set+Unset / Set+Remove keeps the tombstone instead of
/// cancelling.
///
/// - Multiple updates on the same key coalesce to a single final value
/// - Insert+Delete on a never-existing key cancels out (preserves CDC semantics)
/// - Update+Delete on a preexisting key writes a tombstone (preserves correctness)
pub fn optimize_deltas(deltas: impl IntoIterator<Item = Delta>, preexisting_keys: &HashSet<Vec<u8>>) -> Vec<Delta> {
	// Track the optimized state for each key
	// Using IndexMap to preserve insertion order for deterministic CDC sequencing
	let mut key_states: IndexMap<Vec<u8>, (OptimizedDeltaState, usize)> = IndexMap::new();

	// Drop operations are collected separately - they pass through without optimization
	// because they are cleanup operations that work on versioned storage directly
	let mut drop_operations: Vec<(usize, Delta)> = Vec::new();

	for (idx, delta) in deltas.into_iter().enumerate() {
		match delta {
			Delta::Drop {
				key: _,
			} => {
				// Drop operations pass through without optimization
				drop_operations.push((idx, delta));
			}
			Delta::Set {
				key,
				row,
			} => {
				// Check if this key has been seen before in this transaction
				let key_bytes = key.as_ref().to_vec();
				let entry = key_states.entry(key_bytes);
				match entry {
					Occupied(mut occ) => {
						// Key was already modified in this transaction
						let (state, _) = occ.get_mut();
						match state {
							OptimizedDeltaState::Set {
								row: old_row,
							} => {
								// Update + Update = coalesce to final Update
								*old_row = row;
							}
							OptimizedDeltaState::Unset {
								..
							}
							| OptimizedDeltaState::Remove => {
								// Delete + Insert in same transaction = Set
								*state = OptimizedDeltaState::Set {
									row,
								};
							}
							OptimizedDeltaState::Cancelled => {
								// After complete cancellation, treat as new Insert
								*state = OptimizedDeltaState::Set {
									row,
								};
							}
						}
						// Keep the first index - don't update it
					}
					Vacant(vac) => {
						// First time seeing this key in transaction
						vac.insert((
							OptimizedDeltaState::Set {
								row,
							},
							idx,
						));
					}
				}
			}
			Delta::Unset {
				key,
				row,
			} => {
				// Check if this key has been seen before in this transaction
				let key_bytes = key.as_ref().to_vec();
				let preexisting = preexisting_keys.contains(&key_bytes);
				let entry = key_states.entry(key_bytes);
				match entry {
					Occupied(mut occ) => {
						// Key was already modified in this transaction
						let (state, _) = occ.get_mut();
						match state {
							OptimizedDeltaState::Set {
								..
							} => {
								if preexisting {
									// Update + Unset on a prior-committed key:
									// keep the tombstone, otherwise the prior
									// version would remain visible.
									*state = OptimizedDeltaState::Unset {
										row,
									};
								} else {
									// Insert + Unset on a never-existing key:
									// cancel both (CDC sees no event).
									*state = OptimizedDeltaState::Cancelled;
								}
							}
							OptimizedDeltaState::Unset {
								..
							}
							| OptimizedDeltaState::Remove => {
								// Unset + Unset shouldn't happen, but keep the unset
								// Do nothing
							}
							OptimizedDeltaState::Cancelled => {
								// After cancellation, an unset means unset it
								*state = OptimizedDeltaState::Unset {
									row,
								};
							}
						}
						// Keep the first index - don't update it
					}
					Vacant(vac) => {
						// First time seeing this key in transaction - it's an unset
						vac.insert((
							OptimizedDeltaState::Unset {
								row,
							},
							idx,
						));
					}
				}
			}
			Delta::Remove {
				key,
			} => {
				// Check if this key has been seen before in this transaction
				let key_bytes = key.as_ref().to_vec();
				let preexisting = preexisting_keys.contains(&key_bytes);
				let entry = key_states.entry(key_bytes);
				match entry {
					Occupied(mut occ) => {
						// Key was already modified in this transaction
						let (state, _) = occ.get_mut();
						match state {
							OptimizedDeltaState::Set {
								..
							} => {
								if preexisting {
									// Update + Remove on a prior-committed key:
									// keep the Remove (same reasoning as Unset).
									*state = OptimizedDeltaState::Remove;
								} else {
									// Insert + Remove on a never-existing key:
									// cancel both.
									*state = OptimizedDeltaState::Cancelled;
								}
							}
							OptimizedDeltaState::Unset {
								..
							}
							| OptimizedDeltaState::Remove => {
								// Remove + Remove shouldn't happen, but keep the remove
								// Do nothing
							}
							OptimizedDeltaState::Cancelled => {
								// After cancellation, a remove means remove it
								*state = OptimizedDeltaState::Remove;
							}
						}
						// Keep the first index - don't update it
					}
					Vacant(vac) => {
						// First time seeing this key in transaction - it's a remove
						vac.insert((OptimizedDeltaState::Remove, idx));
					}
				}
			}
		}
	}

	// Convert optimized states back to deltas, preserving order
	let mut result: Vec<(usize, Delta)> = Vec::new();

	// Add drop operations (they passed through without optimization)
	result.extend(drop_operations);

	for (key_bytes, (state, idx)) in key_states {
		match state {
			OptimizedDeltaState::Set {
				row,
			} => {
				result.push((
					idx,
					Delta::Set {
						key: EncodedKey(CowVec::new(key_bytes)),
						row,
					},
				));
			}
			OptimizedDeltaState::Unset {
				row,
			} => {
				result.push((
					idx,
					Delta::Unset {
						key: EncodedKey(CowVec::new(key_bytes)),
						row,
					},
				));
			}
			OptimizedDeltaState::Remove => {
				result.push((
					idx,
					Delta::Remove {
						key: EncodedKey(CowVec::new(key_bytes)),
					},
				));
			}
			OptimizedDeltaState::Cancelled => {
				// Skip cancelled operations entirely
			}
		}
	}

	// Sort by original index to maintain order
	result.sort_by_key(|(idx, _)| *idx);

	// Extract just the deltas
	result.into_iter().map(|(_, delta)| delta).collect()
}

#[cfg(test)]
pub mod tests {
	use super::*;

	fn make_key(s: &str) -> EncodedKey {
		EncodedKey(CowVec::new(s.as_bytes().to_vec()))
	}

	fn make_row(s: &str) -> EncodedRow {
		EncodedRow(CowVec::new(s.as_bytes().to_vec()))
	}

	#[test]
	fn test_insert_delete_cancellation() {
		let deltas = vec![
			Delta::Set {
				key: make_key("key_a"),
				row: make_row("value1"),
			},
			Delta::Unset {
				key: make_key("key_a"),
				row: make_row("value1"),
			},
		];

		let optimized = optimize_deltas(deltas, &HashSet::new());

		// Insert + Delete on a never-committed key cancels out completely.
		assert_eq!(optimized.len(), 0);
	}

	#[test]
	fn test_update_delete_keeps_tombstone() {
		let deltas = vec![
			Delta::Set {
				key: make_key("key_a"),
				row: make_row("value1"),
			},
			Delta::Unset {
				key: make_key("key_a"),
				row: make_row("value1"),
			},
		];

		let mut preexisting = HashSet::new();
		preexisting.insert(b"key_a".to_vec());
		let optimized = optimize_deltas(deltas, &preexisting);

		// Update + Delete on a prior-committed key must keep the tombstone -
		// otherwise the prior version remains visible.
		assert_eq!(optimized.len(), 1);
		match &optimized[0] {
			Delta::Unset {
				key,
				row,
			} => {
				assert_eq!(key.as_ref(), b"key_a");
				assert_eq!(row.0.as_slice(), b"value1");
			}
			other => panic!("Expected Unset delta, got {:?}", other),
		}
	}

	#[test]
	fn test_update_remove_keeps_tombstone() {
		let deltas = vec![
			Delta::Set {
				key: make_key("key_a"),
				row: make_row("value1"),
			},
			Delta::Remove {
				key: make_key("key_a"),
			},
		];

		let mut preexisting = HashSet::new();
		preexisting.insert(b"key_a".to_vec());
		let optimized = optimize_deltas(deltas, &preexisting);

		assert_eq!(optimized.len(), 1);
		assert!(matches!(&optimized[0], Delta::Remove { .. }));
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

		// Multiple updates should coalesce to single update
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
			Delta::Unset {
				key: make_key("key_a"),
				row: make_row("value2"),
			},
		];

		let optimized = optimize_deltas(deltas, &HashSet::new());

		// Insert + Update + Delete should cancel
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
			Delta::Unset {
				key: make_key("key_a"),
				row: make_row("value1"),
			},
			Delta::Set {
				key: make_key("key_c"),
				row: make_row("value3"),
			},
		];

		let optimized = optimize_deltas(deltas, &HashSet::new());

		// key_a: Insert+Delete = cancel
		// key_b: Insert = keep
		// key_c: Insert = keep
		assert_eq!(optimized.len(), 2);
	}
}
