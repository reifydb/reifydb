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

#[derive(Debug, Clone)]
enum OptimizedDeltaState {
	Set {
		row: EncodedRow,
	},

	Unset {
		row: EncodedRow,
	},

	Remove,

	Cancelled,
}

pub fn optimize_deltas(deltas: impl IntoIterator<Item = Delta>, preexisting_keys: &HashSet<Vec<u8>>) -> Vec<Delta> {
	let mut key_states: IndexMap<Vec<u8>, (OptimizedDeltaState, usize)> = IndexMap::new();

	let mut drop_operations: Vec<(usize, Delta)> = Vec::new();

	for (idx, delta) in deltas.into_iter().enumerate() {
		match delta {
			Delta::Drop {
				key: _,
			} => {
				drop_operations.push((idx, delta));
			}
			Delta::Set {
				key,
				row,
			} => {
				let key_bytes = key.as_ref().to_vec();
				let entry = key_states.entry(key_bytes);
				match entry {
					Occupied(mut occ) => {
						let (state, _) = occ.get_mut();
						match state {
							OptimizedDeltaState::Set {
								row: old_row,
							} => {
								*old_row = row;
							}
							OptimizedDeltaState::Unset {
								..
							}
							| OptimizedDeltaState::Remove => {
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
			Delta::Unset {
				key,
				row,
			} => {
				let key_bytes = key.as_ref().to_vec();
				let preexisting = preexisting_keys.contains(&key_bytes);
				let entry = key_states.entry(key_bytes);
				match entry {
					Occupied(mut occ) => {
						let (state, _) = occ.get_mut();
						match state {
							OptimizedDeltaState::Set {
								..
							} => {
								if preexisting {
									*state = OptimizedDeltaState::Unset {
										row,
									};
								} else {
									*state = OptimizedDeltaState::Cancelled;
								}
							}
							OptimizedDeltaState::Unset {
								..
							}
							| OptimizedDeltaState::Remove => {}
							OptimizedDeltaState::Cancelled => {
								*state = OptimizedDeltaState::Unset {
									row,
								};
							}
						}
					}
					Vacant(vac) => {
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
				let key_bytes = key.as_ref().to_vec();
				let preexisting = preexisting_keys.contains(&key_bytes);
				let entry = key_states.entry(key_bytes);
				match entry {
					Occupied(mut occ) => {
						let (state, _) = occ.get_mut();
						match state {
							OptimizedDeltaState::Set {
								..
							} => {
								if preexisting {
									*state = OptimizedDeltaState::Remove;
								} else {
									*state = OptimizedDeltaState::Cancelled;
								}
							}
							OptimizedDeltaState::Unset {
								..
							}
							| OptimizedDeltaState::Remove => {}
							OptimizedDeltaState::Cancelled => {
								*state = OptimizedDeltaState::Remove;
							}
						}
					}
					Vacant(vac) => {
						vac.insert((OptimizedDeltaState::Remove, idx));
					}
				}
			}
		}
	}

	let mut result: Vec<(usize, Delta)> = Vec::new();

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
			OptimizedDeltaState::Cancelled => {}
		}
	}

	result.sort_by_key(|(idx, _)| *idx);

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
