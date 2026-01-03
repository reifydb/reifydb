// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use indexmap::{
	IndexMap,
	map::Entry::{Occupied, Vacant},
};
use reifydb_core::{CowVec, EncodedKey, delta::Delta, value::encoded::EncodedValues};

/// Represents the optimized state of a key after all operations in a transaction
#[derive(Debug, Clone)]
enum OptimizedDeltaState {
	/// Key should be set to this value (Insert or Update)
	Set {
		values: EncodedValues,
	},
	/// Key should be removed
	Remove,
	/// Key operations cancelled out (Insert+Delete), skip entirely
	Cancelled,
}

/// Optimize deltas by applying cancellation and coalescing logic at the delta level
///
/// This function processes a sequence of deltas and returns an optimized list where:
/// - Insert+Delete pairs are canceled out completely
/// - Multiple updates are coalesced into a single update
/// - Only the final state for each key is returned
pub(crate) fn optimize_deltas(deltas: impl IntoIterator<Item = Delta>) -> Vec<Delta> {
	// Track the optimized state for each key
	// Using IndexMap to preserve insertion order for deterministic CDC sequencing
	let mut key_states: IndexMap<Vec<u8>, (OptimizedDeltaState, usize)> = IndexMap::new();

	// Drop operations are collected separately - they pass through without optimization
	// because they are cleanup operations that work on versioned storage directly
	let mut drop_operations: Vec<(usize, Delta)> = Vec::new();

	for (idx, delta) in deltas.into_iter().enumerate() {
		match delta {
			Delta::Drop {
				..
			} => {
				// Drop operations pass through without optimization
				drop_operations.push((idx, delta));
			}
			Delta::Set {
				key,
				values,
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
								values: old_values,
							} => {
								// Update + Update = coalesce to final Update
								*old_values = values;
							}
							OptimizedDeltaState::Remove => {
								// Delete + Insert in same transaction = Set
								*state = OptimizedDeltaState::Set {
									values,
								};
							}
							OptimizedDeltaState::Cancelled => {
								// After complete cancellation, treat as new Insert
								*state = OptimizedDeltaState::Set {
									values,
								};
							}
						}
						// Keep the first index - don't update it
					}
					Vacant(vac) => {
						// First time seeing this key in transaction
						vac.insert((
							OptimizedDeltaState::Set {
								values,
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
				let entry = key_states.entry(key_bytes);
				match entry {
					Occupied(mut occ) => {
						// Key was already modified in this transaction
						let (state, _) = occ.get_mut();
						match state {
							OptimizedDeltaState::Set {
								..
							} => {
								// Insert + Delete = Cancel
								*state = OptimizedDeltaState::Cancelled;
							}
							OptimizedDeltaState::Remove => {
								// Delete + Delete shouldn't happen, but keep the delete
								// Do nothing
							}
							OptimizedDeltaState::Cancelled => {
								// After cancellation, a delete means remove it
								*state = OptimizedDeltaState::Remove;
							}
						}
						// Keep the first index - don't update it
					}
					Vacant(vac) => {
						// First time seeing this key in transaction - it's a delete
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
				values,
			} => {
				result.push((
					idx,
					Delta::Set {
						key: EncodedKey(CowVec::new(key_bytes)),
						values,
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
mod tests {
	use super::*;

	fn make_key(s: &str) -> EncodedKey {
		EncodedKey(CowVec::new(s.as_bytes().to_vec()))
	}

	fn make_values(s: &str) -> EncodedValues {
		EncodedValues(CowVec::new(s.as_bytes().to_vec()))
	}

	#[test]
	fn test_insert_delete_cancellation() {
		let deltas = vec![
			Delta::Set {
				key: make_key("key_a"),
				values: make_values("value1"),
			},
			Delta::Remove {
				key: make_key("key_a"),
			},
		];

		let optimized = optimize_deltas(deltas);

		// Insert + Delete should cancel out completely
		assert_eq!(optimized.len(), 0);
	}

	#[test]
	fn test_update_coalescing() {
		let deltas = vec![
			Delta::Set {
				key: make_key("key_a"),
				values: make_values("value1"),
			},
			Delta::Set {
				key: make_key("key_a"),
				values: make_values("value2"),
			},
			Delta::Set {
				key: make_key("key_a"),
				values: make_values("value3"),
			},
		];

		let optimized = optimize_deltas(deltas);

		// Multiple updates should coalesce to single update
		assert_eq!(optimized.len(), 1);
		match &optimized[0] {
			Delta::Set {
				key,
				values,
			} => {
				assert_eq!(key.as_ref(), b"key_a");
				assert_eq!(values.0.as_slice(), b"value3");
			}
			_ => panic!("Expected Set delta"),
		}
	}

	#[test]
	fn test_insert_update_delete() {
		let deltas = vec![
			Delta::Set {
				key: make_key("key_a"),
				values: make_values("value1"),
			},
			Delta::Set {
				key: make_key("key_a"),
				values: make_values("value2"),
			},
			Delta::Remove {
				key: make_key("key_a"),
			},
		];

		let optimized = optimize_deltas(deltas);

		// Insert + Update + Delete should cancel
		assert_eq!(optimized.len(), 0);
	}

	#[test]
	fn test_multiple_keys() {
		let deltas = vec![
			Delta::Set {
				key: make_key("key_a"),
				values: make_values("value1"),
			},
			Delta::Set {
				key: make_key("key_b"),
				values: make_values("value2"),
			},
			Delta::Remove {
				key: make_key("key_a"),
			},
			Delta::Set {
				key: make_key("key_c"),
				values: make_values("value3"),
			},
		];

		let optimized = optimize_deltas(deltas);

		// key_a: Insert+Delete = cancel
		// key_b: Insert = keep
		// key_c: Insert = keep
		assert_eq!(optimized.len(), 2);
	}
}
