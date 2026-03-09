// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::{BTreeMap, HashMap};

use reifydb_core::{
	common::CommitVersion,
	encoded::{
		encoded::EncodedValues,
		key::{EncodedKey, EncodedKeyRange},
	},
	util::encoding::keycode,
};
use reifydb_transaction::multi::transaction::{
	MultiTransaction, read::MultiReadTransaction, write::MultiWriteTransaction,
};
use reifydb_type::util::cowvec::CowVec;

use super::schedule::{Op, Schedule, Step, TxId};

/// The result of executing a single operation.
#[derive(Debug)]
pub enum OpResult {
	Ok,
	Value(Option<Vec<u8>>),
	ScanResult(Vec<(Vec<u8>, Vec<u8>)>),
	Committed,
	Error(String),
}

/// Records the result of a single step execution.
#[derive(Debug)]
pub struct StepResult {
	pub step_index: usize,
	pub tx_id: TxId,
	pub op: Op,
	pub result: OpResult,
}

/// The full trace of a schedule execution.
#[derive(Debug)]
pub struct ExecutionTrace {
	pub results: Vec<StepResult>,
	pub final_state: BTreeMap<String, String>,
	pub committed: HashMap<TxId, CommitVersion>,
}

impl ExecutionTrace {
	/// Returns the result of a Get operation for a specific step, if it was a Value result.
	pub fn get_value(&self, step_index: usize) -> Option<&Option<Vec<u8>>> {
		match &self.results[step_index].result {
			OpResult::Value(v) => Some(v),
			_ => None,
		}
	}
}

enum TxHandle {
	Read(MultiReadTransaction),
	Write(MultiWriteTransaction),
}

pub struct Executor {
	engine: MultiTransaction,
}

fn encode_key(key: &str) -> EncodedKey {
	EncodedKey::new(keycode::serialize(&key.to_string()))
}

fn encode_values(value: &str) -> EncodedValues {
	EncodedValues(CowVec::new(keycode::serialize(&value.to_string())))
}

fn decode_key(bytes: &[u8]) -> String {
	keycode::deserialize::<String>(bytes).unwrap_or_else(|_| format!("<raw:{}>", hex::encode(bytes)))
}

fn decode_values(bytes: &[u8]) -> String {
	keycode::deserialize::<String>(bytes).unwrap_or_else(|_| format!("<raw:{}>", hex::encode(bytes)))
}

mod hex {
	pub fn encode(bytes: &[u8]) -> String {
		bytes.iter().map(|b| format!("{:02x}", b)).collect()
	}
}

impl Executor {
	pub fn new() -> Self {
		Self {
			engine: MultiTransaction::testing(),
		}
	}

	pub fn run(&mut self, schedule: &Schedule) -> ExecutionTrace {
		let mut handles: HashMap<TxId, TxHandle> = HashMap::new();
		let mut results: Vec<StepResult> = Vec::new();
		let mut committed: HashMap<TxId, CommitVersion> = HashMap::new();

		for (step_index, step) in schedule.steps.iter().enumerate() {
			let Step {
				tx_id,
				op,
			} = step;
			let tx_id = *tx_id;

			let result =
				match op {
					Op::BeginCommand => match self.engine.begin_command() {
						Ok(tx) => {
							handles.insert(tx_id, TxHandle::Write(tx));
							OpResult::Ok
						}
						Err(e) => OpResult::Error(format!("{}", e)),
					},
					Op::BeginQuery => match self.engine.begin_query() {
						Ok(rx) => {
							handles.insert(tx_id, TxHandle::Read(rx));
							OpResult::Ok
						}
						Err(e) => OpResult::Error(format!("{}", e)),
					},
					Op::Set {
						key,
						value,
					} => match handles.get_mut(&tx_id) {
						Some(TxHandle::Write(tx)) => {
							match tx.set(&encode_key(key), encode_values(value)) {
								Ok(()) => OpResult::Ok,
								Err(e) => {
									handles.remove(&tx_id);
									OpResult::Error(format!("{}", e))
								}
							}
						}
						Some(TxHandle::Read(_)) => {
							OpResult::Error("cannot set on read transaction".into())
						}
						None => OpResult::Error("transaction not found".into()),
					},
					Op::Get {
						key,
					} => match handles.get_mut(&tx_id) {
						Some(TxHandle::Write(tx)) => match tx.get(&encode_key(key)) {
							Ok(Some(tv)) => OpResult::Value(Some(tv.values().to_vec())),
							Ok(None) => OpResult::Value(None),
							Err(e) => {
								handles.remove(&tx_id);
								OpResult::Error(format!("{}", e))
							}
						},
						Some(TxHandle::Read(rx)) => match rx.get(&encode_key(key)) {
							Ok(Some(tv)) => OpResult::Value(Some(tv.values().to_vec())),
							Ok(None) => OpResult::Value(None),
							Err(e) => {
								handles.remove(&tx_id);
								OpResult::Error(format!("{}", e))
							}
						},
						None => OpResult::Error("transaction not found".into()),
					},
					Op::Remove {
						key,
					} => match handles.get_mut(&tx_id) {
						Some(TxHandle::Write(tx)) => match tx.remove(&encode_key(key)) {
							Ok(()) => OpResult::Ok,
							Err(e) => {
								handles.remove(&tx_id);
								OpResult::Error(format!("{}", e))
							}
						},
						Some(TxHandle::Read(_)) => {
							OpResult::Error("cannot remove on read transaction".into())
						}
						None => OpResult::Error("transaction not found".into()),
					},
					Op::Scan => {
						match handles.get_mut(&tx_id) {
							Some(TxHandle::Write(tx)) => {
								match tx.range(EncodedKeyRange::all(), 1024)
									.collect::<Result<Vec<_>, _>>()
								{
									Ok(items) => {
										let pairs =
											items.iter()
												.map(|mv| {
													(mv.key.as_ref().to_vec(), mv.values.to_vec())
												})
												.collect();
										OpResult::ScanResult(pairs)
									}
									Err(e) => {
										handles.remove(&tx_id);
										OpResult::Error(format!("{}", e))
									}
								}
							}
							Some(TxHandle::Read(rx)) => {
								match rx.range(EncodedKeyRange::all(), 1024)
									.collect::<Result<Vec<_>, _>>()
								{
									Ok(items) => {
										let pairs =
											items.iter()
												.map(|mv| {
													(mv.key.as_ref().to_vec(), mv.values.to_vec())
												})
												.collect();
										OpResult::ScanResult(pairs)
									}
									Err(e) => {
										handles.remove(&tx_id);
										OpResult::Error(format!("{}", e))
									}
								}
							}
							None => OpResult::Error("transaction not found".into()),
						}
					}
					Op::Commit => match handles.remove(&tx_id) {
						Some(TxHandle::Write(mut tx)) => match tx.commit() {
							Ok(version) => {
								committed.insert(tx_id, version);
								OpResult::Committed
							}
							Err(e) => OpResult::Error(format!("{}", e)),
						},
						Some(TxHandle::Read(_)) => {
							OpResult::Error("cannot commit read transaction".into())
						}
						None => OpResult::Error("transaction not found".into()),
					},
					Op::Rollback => match handles.remove(&tx_id) {
						Some(TxHandle::Write(mut tx)) => match tx.rollback() {
							Ok(()) => OpResult::Ok,
							Err(e) => OpResult::Error(format!("{}", e)),
						},
						Some(TxHandle::Read(_)) => OpResult::Ok,
						None => OpResult::Error("transaction not found".into()),
					},
				};

			results.push(StepResult {
				step_index,
				tx_id,
				op: op.clone(),
				result,
			});
		}

		// Drop remaining handles (uncommitted transactions are implicitly rolled back)
		drop(handles);

		// Read final state via a fresh read transaction
		let final_state = self.read_final_state();

		ExecutionTrace {
			results,
			final_state,
			committed,
		}
	}

	fn read_final_state(&self) -> BTreeMap<String, String> {
		let rx = self.engine.begin_query().unwrap();
		let items: Vec<_> = rx.range(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();

		let mut state = BTreeMap::new();
		for mv in items {
			let key = decode_key(mv.key.as_ref());
			let value = decode_values(mv.values.as_ref());
			state.insert(key, value);
		}
		state
	}
}
