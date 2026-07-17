// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashSet, sync::Arc};

use indexmap::IndexMap;
use postcard::{from_bytes, to_stdvec};
use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_codec::{encoded::shape::RowShape, key::encoded::EncodedKey};
use reifydb_core::{
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diff},
	},
	value::column::columns::Columns,
};
use reifydb_engine::expression::{
	compile::{CompiledExpr, compile_expression},
	context::CompileContext,
};
use reifydb_routine::routine::registry::Routines;
use reifydb_rql::expression::Expression;
use reifydb_runtime::context::RuntimeContext;
use reifydb_sdk::operator::Tick;
use reifydb_value::{
	Result,
	error::Error,
	util::hash::Hash128,
	value::{blob::Blob, duration::Duration},
};

use crate::{
	context::FlowContext,
	error::FlowStateError,
	operator::{
		Operator, OperatorCell,
		distinct::state::{DistinctEntry, DistinctLayout, DistinctState},
		stateful::{raw::RawStatefulOperator, row::RowNumberProvider, single::SingleStateful, utils},
	},
	transaction::{FlowTransaction, slot::PersistFn},
};

const ENTRY_KEY_PREFIX: u8 = 0x01;
const LAYOUT_KEY_PREFIX: u8 = 0x02;

struct DistinctWorkingSet {
	state: DistinctState,
	loaded: HashSet<Hash128>,
}

pub struct DistinctOperator {
	parent: OperatorCell,
	pub(super) node: FlowNodeId,
	pub(super) compiled_expressions: Vec<CompiledExpr>,
	pub(super) shape: RowShape,
	pub(super) routines: Routines,
	pub(super) runtime_context: RuntimeContext,
	pub(super) ttl_nanos: Option<u64>,
	pub(super) row_number_provider: RowNumberProvider,
	pub(super) ctx: Arc<FlowContext>,
}

impl DistinctOperator {
	pub fn new(
		parent: OperatorCell,
		node: FlowNodeId,
		expressions: Vec<Expression>,
		routines: Routines,
		runtime_context: RuntimeContext,
		ttl_nanos: Option<u64>,
		ctx: Arc<FlowContext>,
	) -> Self {
		let compile_ctx = CompileContext {
			symbols: &ctx.symbols,
		};
		let compiled_expressions: Vec<CompiledExpr> = expressions
			.iter()
			.map(|e| compile_expression(&compile_ctx, e))
			.collect::<Result<Vec<_>>>()
			.expect("Failed to compile expressions");

		Self {
			parent,
			node,
			compiled_expressions,
			shape: RowShape::operator_state(),
			routines,
			runtime_context,
			ttl_nanos,
			row_number_provider: RowNumberProvider::new(node),
			ctx,
		}
	}

	pub(crate) fn output_schema(&self) -> Option<Columns> {
		self.parent.output_schema()
	}

	pub(super) fn entry_key(hash: Hash128) -> EncodedKey {
		let mut bytes = Vec::with_capacity(1 + 16);
		bytes.push(ENTRY_KEY_PREFIX);
		bytes.extend_from_slice(&hash.0.to_be_bytes());
		EncodedKey::new(bytes)
	}

	fn layout_storage_key() -> EncodedKey {
		EncodedKey::new(vec![LAYOUT_KEY_PREFIX])
	}

	pub(super) fn hash_from_entry_key(key: &[u8]) -> Option<Hash128> {
		if key.first() != Some(&ENTRY_KEY_PREFIX) || key.len() != 1 + 16 {
			return None;
		}
		let mut bytes = [0u8; 16];
		bytes.copy_from_slice(&key[1..17]);
		Some(Hash128(u128::from_be_bytes(bytes)))
	}

	fn load_entry(&self, txn: &mut FlowTransaction, hash: Hash128) -> Result<Option<DistinctEntry>> {
		match utils::state_get(self.node, txn, &Self::entry_key(hash))? {
			Some(row) => {
				let blob = self.shape.get_blob(&row, 0);
				if blob.is_empty() {
					return Ok(None);
				}
				let entry: DistinctEntry = from_bytes(blob.as_ref()).map_err(|e| {
					Error::from(FlowStateError::Decode {
						state: "DistinctEntry",
						cause: e.to_string(),
					})
				})?;
				Ok(Some(entry))
			}
			None => Ok(None),
		}
	}

	fn load_layout(&self, txn: &mut FlowTransaction) -> Result<DistinctLayout> {
		match utils::state_get(self.node, txn, &Self::layout_storage_key())? {
			Some(row) => {
				let blob = self.shape.get_blob(&row, 0);
				if blob.is_empty() {
					return Ok(DistinctLayout::new());
				}
				from_bytes(blob.as_ref()).map_err(|e| {
					Error::from(FlowStateError::Decode {
						state: "DistinctLayout",
						cause: e.to_string(),
					})
				})
			}
			None => Ok(DistinctLayout::new()),
		}
	}

	#[cfg(test)]
	pub(super) fn count_entries(&self, txn: &mut FlowTransaction) -> usize {
		utils::state_scan_all(self.node, txn)
			.unwrap()
			.iter()
			.filter(|(k, _)| Self::hash_from_entry_key(k.as_ref()).is_some())
			.count()
	}

	fn batch_hashes(&self, diffs: &[Diff]) -> Result<HashSet<Hash128>> {
		let mut touched: HashSet<Hash128> = HashSet::new();
		for diff in diffs {
			match diff {
				Diff::Insert {
					post,
					..
				} => touched.extend(self.compute_hashes(post)?),
				Diff::Update {
					pre,
					post,
					..
				} => {
					touched.extend(self.compute_hashes(pre)?);
					touched.extend(self.compute_hashes(post)?);
				}
				Diff::Remove {
					pre,
					..
				} => touched.extend(self.compute_hashes(pre)?),
			}
		}
		Ok(touched)
	}
}

impl RawStatefulOperator for DistinctOperator {}

impl SingleStateful for DistinctOperator {
	fn layout(&self) -> RowShape {
		self.shape.clone()
	}
}

impl Operator for DistinctOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD_WITH_TICK
	}

	fn ticks(&self) -> Option<Duration> {
		self.ticks_interval()
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let node_id = self.node;
		let shape = self.shape.clone();
		let touched = self.batch_hashes(&change.diffs)?;

		let (mut working, persist) = txn.take_operator_state::<DistinctWorkingSet, _>(node_id, |txn| {
			let layout = self.load_layout(txn)?;
			let working = DistinctWorkingSet {
				state: DistinctState {
					entries: IndexMap::new(),
					layout,
				},
				loaded: HashSet::new(),
			};
			let persist: PersistFn = Box::new(move |txn, value| {
				let working =
					*value.downcast::<DistinctWorkingSet>().expect("DistinctWorkingSet slot type");
				for hash in &working.loaded {
					let key = Self::entry_key(*hash);
					match working.state.entries.get(hash) {
						Some(entry) => {
							let bytes = to_stdvec(entry).map_err(|e| {
								Error::from(FlowStateError::Encode {
									state: "DistinctEntry",
									cause: e.to_string(),
								})
							})?;
							let mut row = shape.allocate();
							shape.set_blob(&mut row, 0, &Blob::from(bytes));
							utils::state_set(node_id, txn, &key, row)?;
						}
						None => utils::state_drop(node_id, txn, &key)?,
					}
				}
				let layout_bytes = to_stdvec(&working.state.layout).map_err(|e| {
					Error::from(FlowStateError::Encode {
						state: "DistinctLayout",
						cause: e.to_string(),
					})
				})?;
				let mut layout_row = shape.allocate();
				shape.set_blob(&mut layout_row, 0, &Blob::from(layout_bytes));
				utils::state_set(node_id, txn, &Self::layout_storage_key(), layout_row)?;
				Ok(())
			});
			Ok((working, persist))
		})?;

		for &hash in &touched {
			if working.loaded.insert(hash)
				&& let Some(entry) = self.load_entry(txn, hash)?
			{
				working.state.entries.insert(hash, entry);
			}
		}

		let mut result = Vec::new();
		for diff in change.diffs {
			match diff {
				Diff::Insert {
					post,
					..
				} => {
					let insert_result = self.process_insert(txn, &mut working.state, &post)?;
					result.extend(insert_result);
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					let update_result =
						self.process_update(txn, &mut working.state, &pre, &post)?;
					result.extend(update_result);
				}
				Diff::Remove {
					pre,
					..
				} => {
					let remove_result = self.process_remove(txn, &mut working.state, &pre)?;
					result.extend(remove_result);
				}
			}
		}

		txn.put_operator_state(node_id, working, persist);

		Ok(Change::from_flow(self.node, change.version, result, change.changed_at))
	}

	fn tick(&self, txn: &mut FlowTransaction, tick: Tick) -> Result<Option<Change>> {
		self.tick_evict(txn, tick)
	}
}
