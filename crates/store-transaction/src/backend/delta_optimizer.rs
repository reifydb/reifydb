// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashMap;

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
	/// Key was deleted then re-inserted - treat as Insert even if key exists in storage
	SetAfterDelete {
		values: EncodedValues,
	},
}

/// Optimize deltas by applying cancellation and coalescing logic at the delta level
///
/// This function processes a sequence of deltas and returns an optimized list where:
/// - Insert+Delete pairs are cancelled out completely
/// - Multiple updates are coalesced into a single update
/// - Only the final state for each key is returned
///
/// This optimization happens BEFORE database writes, reducing unnecessary I/O operations.
pub(crate) fn optimize_deltas<F>(deltas: impl IntoIterator<Item = Delta>, mut key_exists_in_storage: F) -> Vec<Delta>
where
	F: FnMut(&EncodedKey) -> bool,
{
	// Track the optimized state for each key
	let mut key_states: HashMap<EncodedKey, (OptimizedDeltaState, usize)> = HashMap::new();

	for (idx, delta) in deltas.into_iter().enumerate() {
		match delta {
			Delta::Set {
				key,
				values,
			} => {
				// Check if this key has been seen before in this transaction
				let entry = key_states.entry(key.clone());
				match entry {
					std::collections::hash_map::Entry::Occupied(mut occ) => {
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
								// Delete + Insert in same transaction
								// Mark as SetAfterDelete so CDC knows to treat as
								// Insert
								let existed = key_exists_in_storage(&key);
								if existed {
									*state = OptimizedDeltaState::SetAfterDelete {
										values,
									};
								} else {
									*state = OptimizedDeltaState::Set {
										values,
									};
								}
							}
							OptimizedDeltaState::Cancelled => {
								// After complete cancellation, treat as new Insert
								*state = OptimizedDeltaState::Set {
									values,
								};
							}
							OptimizedDeltaState::SetAfterDelete {
								values: old_values,
							} => {
								// SetAfterDelete + Update = still SetAfterDelete with
								// new value
								*old_values = values;
							}
						}
						// Keep the first index - don't update it
					}
					std::collections::hash_map::Entry::Vacant(vac) => {
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
				let entry = key_states.entry(key.clone());
				match entry {
					std::collections::hash_map::Entry::Occupied(mut occ) => {
						// Key was already modified in this transaction
						let (state, _) = occ.get_mut();
						match state {
							OptimizedDeltaState::Set {
								..
							} => {
								// Check if this was an insert or update
								let existed = key_exists_in_storage(&key);
								if existed {
									// Storage had this key, so Update+Delete =
									// Delete
									*state = OptimizedDeltaState::Remove;
								} else {
									// Key didn't exist in storage, so Insert+Delete
									// = Cancel
									*state = OptimizedDeltaState::Cancelled;
								}
							}
							OptimizedDeltaState::Remove => {
								// Delete + Delete shouldn't happen, but keep the delete
								// Do nothing
							}
							OptimizedDeltaState::Cancelled => {
								// After cancellation, a delete means remove it
								*state = OptimizedDeltaState::Remove;
							}
							OptimizedDeltaState::SetAfterDelete {
								..
							} => {
								// SetAfterDelete + Delete = just Delete
								*state = OptimizedDeltaState::Remove;
							}
						}
						// Keep the first index - don't update it
					}
					std::collections::hash_map::Entry::Vacant(vac) => {
						// First time seeing this key in transaction - it's a delete
						vac.insert((OptimizedDeltaState::Remove, idx));
					}
				}
			}
		}
	}

	// Convert optimized states back to deltas, preserving order
	let mut result: Vec<(usize, Delta)> = Vec::new();

	for (key, (state, idx)) in key_states {
		match state {
			OptimizedDeltaState::Set {
				values,
			} => {
				result.push((
					idx,
					Delta::Set {
						key,
						values,
					},
				));
			}
			OptimizedDeltaState::SetAfterDelete {
				values,
			} => {
				// Emit both Delete and Set for CDC to see the pattern
				// This is still optimized (2 ops instead of many)
				result.push((
					idx,
					Delta::Remove {
						key: key.clone(),
					},
				));
				result.push((
					idx,
					Delta::Set {
						key,
						values,
					},
				));
			}
			OptimizedDeltaState::Remove => {
				result.push((
					idx,
					Delta::Remove {
						key,
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

/// Convenience wrapper that takes CowVec and returns CowVec
pub(crate) fn optimize_deltas_cow<F>(deltas: CowVec<Delta>, key_exists_in_storage: F) -> CowVec<Delta>
where
	F: FnMut(&EncodedKey) -> bool,
{
	let optimized = optimize_deltas(deltas, key_exists_in_storage);
	CowVec::new(optimized)
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::encoded::EncodedValues;

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

		let optimized = optimize_deltas(deltas, |_| false);

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

		let optimized = optimize_deltas(deltas, |_| true);

		// Multiple updates should coalesce to single update
		assert_eq!(optimized.len(), 1);
		match &optimized[0] {
			Delta::Set {
				key,
				values,
			} => {
				assert_eq!(key.0.as_slice(), b"key_a");
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

		let optimized = optimize_deltas(deltas, |_| false);

		// Insert + Update + Delete (when key doesn't exist) should cancel
		assert_eq!(optimized.len(), 0);
	}

	#[test]
	fn test_update_delete_existing_key() {
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

		let optimized = optimize_deltas(deltas, |_| true); // Key exists in storage

		// Update + Update + Delete should result in Delete
		assert_eq!(optimized.len(), 1);
		match &optimized[0] {
			Delta::Remove {
				key,
			} => {
				assert_eq!(key.0.as_slice(), b"key_a");
			}
			_ => panic!("Expected Remove delta"),
		}
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

		let optimized = optimize_deltas(deltas, |_| false);

		// key_a: Insert+Delete = cancel
		// key_b: Insert = keep
		// key_c: Insert = keep
		assert_eq!(optimized.len(), 2);
	}
}
