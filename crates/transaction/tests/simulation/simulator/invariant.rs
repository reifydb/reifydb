// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::{BTreeMap, HashMap, HashSet};

use reifydb_core::util::encoding::keycode;

use super::{
	executor::{ExecutionTrace, OpResult},
	schedule::{Op, TxId},
};

#[derive(Debug)]
pub struct InvariantViolation {
	pub invariant_name: String,
	pub message: String,
}

impl std::fmt::Display for InvariantViolation {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "[{}] {}", self.invariant_name, self.message)
	}
}

pub trait Invariant: std::fmt::Debug {
	fn check(&self, trace: &ExecutionTrace) -> Result<(), InvariantViolation>;
}

/// Verifies that no transaction reads a value written by another uncommitted transaction.
///
/// Under snapshot isolation, reads see the snapshot at begin time plus own writes.
/// A dirty read would be seeing an uncommitted write from another transaction.
/// Tracks `Remove` ops as pending writes (tombstones) so that a read after another
/// transaction's uncommitted remove is also caught.
#[derive(Debug)]
pub struct NoDirtyReads;

impl Invariant for NoDirtyReads {
	fn check(&self, trace: &ExecutionTrace) -> Result<(), InvariantViolation> {
		// committed_state[key] = encoded value bytes. Absent = key doesn't exist.
		let mut committed_state: HashMap<String, Vec<u8>> = HashMap::new();
		// Snapshot of committed state at each tx's begin
		let mut tx_snapshots: HashMap<TxId, HashMap<String, Vec<u8>>> = HashMap::new();
		// Pending writes per tx: key -> Some(encoded_value) for Set, None for Remove (tombstone)
		let mut pending_writes: HashMap<TxId, HashMap<String, Option<Vec<u8>>>> = HashMap::new();

		for result in &trace.results {
			let tx_id = result.tx_id;

			match &result.op {
				Op::BeginCommand | Op::BeginQuery => {
					if matches!(&result.result, OpResult::Ok) {
						tx_snapshots.insert(tx_id, committed_state.clone());
					}
				}
				Op::Set {
					key,
					value,
				} => {
					if matches!(&result.result, OpResult::Ok) {
						let value_bytes = keycode::serialize(&value.to_string());
						pending_writes
							.entry(tx_id)
							.or_default()
							.insert(key.clone(), Some(value_bytes));
					}
				}
				Op::Remove {
					key,
				} => {
					if matches!(&result.result, OpResult::Ok) {
						pending_writes.entry(tx_id).or_default().insert(key.clone(), None);
					}
				}
				Op::Get {
					key,
				} => {
					if let OpResult::Value(read_val) = &result.result {
						let pending = pending_writes.get(&tx_id).and_then(|w| w.get(key));
						let expected: Option<Vec<u8>> = match pending {
							Some(Some(bytes)) => Some(bytes.clone()),
							Some(None) => None, // tx removed this key
							None => tx_snapshots
								.get(&tx_id)
								.and_then(|s| s.get(key))
								.cloned(),
						};

						match (read_val, &expected) {
							(None, None) => {}
							(Some(read_bytes), Some(expected_bytes)) => {
								if read_bytes != expected_bytes {
									return Err(InvariantViolation {
										invariant_name: "NoDirtyReads".into(),
										message: format!(
											"tx {:?} read unexpected value for key '{}' at step {}",
											tx_id, key, result.step_index
										),
									});
								}
							}
							(None, Some(_)) => {
								return Err(InvariantViolation {
									invariant_name: "NoDirtyReads".into(),
									message: format!(
										"tx {:?} read None for key '{}' at step {} but expected a value",
										tx_id, key, result.step_index
									),
								});
							}
							(Some(_), None) => {
								return Err(InvariantViolation {
									invariant_name: "NoDirtyReads".into(),
									message: format!(
										"tx {:?} read a value for key '{}' at step {} but expected None — possible dirty read",
										tx_id, key, result.step_index
									),
								});
							}
						}
					}
				}
				Op::Commit => {
					if let OpResult::Committed = &result.result {
						if let Some(writes) = pending_writes.remove(&tx_id) {
							for (key, val) in writes {
								match val {
									Some(bytes) => {
										committed_state.insert(key, bytes);
									}
									None => {
										committed_state.remove(&key);
									}
								}
							}
						}
					}
				}
				Op::Rollback => {
					pending_writes.remove(&tx_id);
				}
				_ => {}
			}
		}

		Ok(())
	}
}

/// Verifies that if two *concurrent* transactions write the same key, at least one must abort.
///
/// Two transactions are concurrent if their lifetimes overlap: tx_a began before tx_b
/// committed AND tx_b began before tx_a committed. Sequential (non-overlapping) transactions
/// writing the same key is perfectly valid.
#[derive(Debug)]
pub struct NoLostUpdates;

impl Invariant for NoLostUpdates {
	fn check(&self, trace: &ExecutionTrace) -> Result<(), InvariantViolation> {
		let mut tx_begin_step: HashMap<TxId, usize> = HashMap::new();
		let mut tx_commit_step: HashMap<TxId, usize> = HashMap::new();
		let mut tx_write_keys: HashMap<TxId, HashSet<String>> = HashMap::new();

		for result in &trace.results {
			match &result.op {
				Op::BeginCommand | Op::BeginQuery => {
					tx_begin_step.insert(result.tx_id, result.step_index);
				}
				Op::Set {
					key,
					..
				} => {
					if matches!(&result.result, OpResult::Ok) {
						tx_write_keys.entry(result.tx_id).or_default().insert(key.clone());
					}
				}
				Op::Remove {
					key,
				} => {
					if matches!(&result.result, OpResult::Ok) {
						tx_write_keys.entry(result.tx_id).or_default().insert(key.clone());
					}
				}
				Op::Commit => {
					if let OpResult::Committed = &result.result {
						tx_commit_step.insert(result.tx_id, result.step_index);
					}
				}
				_ => {}
			}
		}

		let committed_writers: Vec<_> =
			tx_write_keys.iter().filter(|(tx_id, _)| trace.committed.contains_key(tx_id)).collect();

		for i in 0..committed_writers.len() {
			for j in (i + 1)..committed_writers.len() {
				let (tx_a, keys_a) = committed_writers[i];
				let (tx_b, keys_b) = committed_writers[j];

				let a_begin = tx_begin_step.get(tx_a).copied().unwrap_or(0);
				let a_commit = tx_commit_step.get(tx_a).copied().unwrap_or(usize::MAX);
				let b_begin = tx_begin_step.get(tx_b).copied().unwrap_or(0);
				let b_commit = tx_commit_step.get(tx_b).copied().unwrap_or(usize::MAX);

				let concurrent = a_begin < b_commit && b_begin < a_commit;
				if !concurrent {
					continue;
				}

				for key in keys_a.intersection(keys_b) {
					return Err(InvariantViolation {
						invariant_name: "NoLostUpdates".into(),
						message: format!(
							"concurrent tx {:?} and tx {:?} both committed writes to key '{}'",
							tx_a, tx_b, key
						),
					});
				}
			}
		}

		Ok(())
	}
}

/// User-supplied predicate on the final state.
pub struct FinalStateConsistency {
	pub name: String,
	pub predicate: Box<dyn Fn(&BTreeMap<String, String>, &ExecutionTrace) -> Result<(), String>>,
}

impl std::fmt::Debug for FinalStateConsistency {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("FinalStateConsistency").field("name", &self.name).finish()
	}
}

impl Invariant for FinalStateConsistency {
	fn check(&self, trace: &ExecutionTrace) -> Result<(), InvariantViolation> {
		(self.predicate)(&trace.final_state, trace).map_err(|msg| InvariantViolation {
			invariant_name: format!("FinalStateConsistency({})", self.name),
			message: msg,
		})
	}
}

/// Verifies that within a single transaction, reads reflect own prior writes.
///
/// A `Get` after a `Set` on the same key must return the set value.
/// A `Get` after a `Remove` on the same key must return `None`.
#[derive(Debug)]
pub struct ReadYourOwnWrites;

impl Invariant for ReadYourOwnWrites {
	fn check(&self, trace: &ExecutionTrace) -> Result<(), InvariantViolation> {
		let mut pending_writes: HashMap<TxId, HashMap<String, Option<Vec<u8>>>> = HashMap::new();

		for result in &trace.results {
			let tx_id = result.tx_id;

			match &result.op {
				Op::Set {
					key,
					value,
				} => {
					if matches!(&result.result, OpResult::Ok) {
						let value_bytes = keycode::serialize(&value.to_string());
						pending_writes
							.entry(tx_id)
							.or_default()
							.insert(key.clone(), Some(value_bytes));
					}
				}
				Op::Remove {
					key,
				} => {
					if matches!(&result.result, OpResult::Ok) {
						pending_writes.entry(tx_id).or_default().insert(key.clone(), None);
					}
				}
				Op::Get {
					key,
				} => {
					if let OpResult::Value(read_val) = &result.result {
						if let Some(expected) =
							pending_writes.get(&tx_id).and_then(|w| w.get(key))
						{
							match (read_val, expected) {
								(None, None) => {}
								(Some(read_bytes), Some(expected_bytes)) => {
									if read_bytes != expected_bytes {
										return Err(InvariantViolation {
											invariant_name:
												"ReadYourOwnWrites"
													.into(),
											message: format!(
												"tx {:?} wrote key '{}' but read back different value at step {}",
												tx_id,
												key,
												result.step_index
											),
										});
									}
								}
								(Some(_), None) => {
									return Err(InvariantViolation {
										invariant_name: "ReadYourOwnWrites"
											.into(),
										message: format!(
											"tx {:?} removed key '{}' but read back a value at step {}",
											tx_id, key, result.step_index
										),
									});
								}
								(None, Some(_)) => {
									return Err(InvariantViolation {
										invariant_name: "ReadYourOwnWrites"
											.into(),
										message: format!(
											"tx {:?} wrote key '{}' but read back None at step {}",
											tx_id, key, result.step_index
										),
									});
								}
							}
						}
					}
				}
				Op::Commit | Op::Rollback => {
					pending_writes.remove(&tx_id);
				}
				_ => {}
			}
		}

		Ok(())
	}
}

/// Verifies that `Scan` results match the expected snapshot plus own writes.
///
/// Maintains committed state and per-transaction pending writes. On each `Scan`,
/// computes the expected visible key set (snapshot merged with own writes) and
/// compares it against the actual scan result.
#[derive(Debug)]
pub struct SnapshotConsistency;

impl Invariant for SnapshotConsistency {
	fn check(&self, trace: &ExecutionTrace) -> Result<(), InvariantViolation> {
		// committed_state[key] = value_string. Absent = key doesn't exist.
		let mut committed_state: BTreeMap<String, String> = BTreeMap::new();
		// Snapshot at each tx's begin
		let mut tx_snapshots: HashMap<TxId, BTreeMap<String, String>> = HashMap::new();
		// Pending writes per tx: key -> Some(value_string) for Set, None for Remove
		let mut pending_writes: HashMap<TxId, HashMap<String, Option<String>>> = HashMap::new();

		for result in &trace.results {
			let tx_id = result.tx_id;

			match &result.op {
				Op::BeginCommand | Op::BeginQuery => {
					if matches!(&result.result, OpResult::Ok) {
						tx_snapshots.insert(tx_id, committed_state.clone());
					}
				}
				Op::Set {
					key,
					value,
				} => {
					if matches!(&result.result, OpResult::Ok) {
						pending_writes
							.entry(tx_id)
							.or_default()
							.insert(key.clone(), Some(value.clone()));
					}
				}
				Op::Remove {
					key,
				} => {
					if matches!(&result.result, OpResult::Ok) {
						pending_writes.entry(tx_id).or_default().insert(key.clone(), None);
					}
				}
				Op::Scan => {
					if let OpResult::ScanResult(pairs) = &result.result {
						// Compute expected: snapshot + pending writes
						let mut expected =
							tx_snapshots.get(&tx_id).cloned().unwrap_or_default();
						if let Some(writes) = pending_writes.get(&tx_id) {
							for (key, val) in writes {
								match val {
									Some(v) => {
										expected.insert(key.clone(), v.clone());
									}
									None => {
										expected.remove(key);
									}
								}
							}
						}

						// Decode actual scan result
						let mut actual: BTreeMap<String, String> = BTreeMap::new();
						for (k_bytes, v_bytes) in pairs {
							let key = keycode::deserialize::<String>(k_bytes)
								.unwrap_or_else(|_| format!("<raw:{}>", k_bytes.len()));
							let value = keycode::deserialize::<String>(v_bytes)
								.unwrap_or_else(|_| format!("<raw:{}>", v_bytes.len()));
							actual.insert(key, value);
						}

						if actual != expected {
							return Err(InvariantViolation {
								invariant_name: "SnapshotConsistency".into(),
								message: format!(
									"tx {:?} scan at step {} returned unexpected results.\n  expected: {:?}\n  actual:   {:?}",
									tx_id, result.step_index, expected, actual,
								),
							});
						}
					}
				}
				Op::Commit => {
					if let OpResult::Committed = &result.result {
						if let Some(writes) = pending_writes.remove(&tx_id) {
							for (key, val) in writes {
								match val {
									Some(v) => {
										committed_state.insert(key, v);
									}
									None => {
										committed_state.remove(&key);
									}
								}
							}
						}
					}
				}
				Op::Rollback => {
					pending_writes.remove(&tx_id);
				}
				_ => {}
			}
		}

		Ok(())
	}
}
